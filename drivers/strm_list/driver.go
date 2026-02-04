package strm_list

import (
	"bufio"
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	// 必须导入的SQLite3驱动（CGO依赖）
	_ "github.com/mattn/go-sqlite3"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	log "github.com/sirupsen/logrus"
)

// 驱动配置项 - 后台添加驱动时可配置，默认值适配OpenList目录规范
type Addition struct {
	TxtPath string `json:"txt_path" config:"strm.txt 路径" default:"/opt/openlist/strm.txt" help:"strm文件绝对路径，格式：路径#播放URL，每行一条"`
	DbPath  string `json:"db_path" config:"SQLite数据库路径" default:"/opt/openlist/strm.db" help:"SQLite数据库绝对路径，需保证OpenList有读写权限"`
}

// 驱动元数据 - 框架要求的基础配置
var config = driver.Config{
	Name:        "StrmList",       // 驱动名称（后台显示）
	LocalSort:   true,             // 本地排序
	NoCache:     false,            // 启用缓存
	DefaultRoot: "/",              // 根路径，与IRootPath保持一致
	Description: "解析strm文件的自定义驱动，支持路径#URL格式批量导入",
}

// 驱动核心结构体 - 嵌入model.Storage，实现所有必要接口
type StrmList struct {
	model.Storage
	Addition // 配置项
	db       *sql.DB // SQLite数据库连接池
}

// ########### 实现框架强制要求的IRootPath接口（解决根目录报错）###########
func (d *StrmList) GetRootPath() string {
	return "/" // 根路径固定为/，贴合现有路径解析逻辑
}

// ########### 实现driver.Driver接口 - 返回驱动配置 ###########
func (d *StrmList) Config() driver.Config {
	return config
}

// ########### 实现driver.Driver接口 - 返回自定义配置项 ###########
func (d *StrmList) GetAddition() driver.Additional {
	return &d.Addition
}

// ########### 实现driver.Driver接口 - GetStorage指针返回 ###########
func (d *StrmList) GetStorage() *model.Storage {
	return &d.Storage
}

// ########### 驱动初始化 - 连接SQLite、建表、异步导入strm.txt ###########
func (d *StrmList) Init(ctx context.Context) error {
	// 校验配置项非空
	if d.DbPath == "" || d.TxtPath == "" {
		return fmt.Errorf("配置错误：strm.txt路径和数据库路径不能为空")
	}

	// 确保数据库目录存在并赋予读写权限
	dbDir := filepath.Dir(d.DbPath)
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return fmt.Errorf("创建数据库目录失败: %v", err)
	}

	// 连接SQLite3（修正驱动名为sqlite3，添加性能参数）
	dsn := d.DbPath + "?_pragma=journal_mode(OFF)&_pragma=synchronous(OFF)&_pragma=cache_size=-10000"
	var err error
	d.db, err = sql.Open("sqlite3", dsn)
	if err != nil {
		return fmt.Errorf("打开SQLite3失败: %v", err)
	}

	// 测试实际连接（sql.Open仅创建连接池，不实际连接）
	if err := d.db.Ping(); err != nil {
		d.db.Close()
		return fmt.Errorf("SQLite3连接测试失败: %v", err)
	}

	// SQLite单文件特性，设置连接池为1避免锁冲突
	d.db.SetMaxOpenConns(1)
	d.db.SetMaxIdleConns(1)
	d.db.SetConnMaxLifetime(0)

	// 初始化表结构和索引
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS nodes (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		name TEXT NOT NULL,
		parent_id INTEGER NOT NULL DEFAULT 0,
		is_dir BOOLEAN NOT NULL DEFAULT 0,
		content TEXT NOT NULL DEFAULT ''
	);
	CREATE INDEX IF NOT EXISTS idx_parent_name ON nodes(parent_id, name);
	`
	if _, err := d.db.Exec(createTableSQL); err != nil {
		d.db.Close()
		return fmt.Errorf("初始化表结构失败: %v", err)
	}

	// 空库时异步导入strm.txt（仅根节点id=0时导入）
	var nodeCount int
	_ = d.db.QueryRow("SELECT COUNT(*) FROM nodes").Scan(&nodeCount)
	if nodeCount <= 1 {
		go d.importStrmTask() // 异步导入，不阻塞驱动初始化
	}

	log.Infof("StrmList驱动初始化成功，数据库：%s，strm.txt：%s", d.DbPath, d.TxtPath)
	return nil
}

// ########### 驱动销毁 - 关闭SQLite连接 ###########
func (d *StrmList) Drop(ctx context.Context) error {
	if d.db != nil {
		if err := d.db.Close(); err != nil {
			log.Warnf("关闭SQLite3连接失败: %v", err)
			return err
		}
		log.Infof("StrmList驱动销毁，已关闭数据库连接")
	}
	return nil
}

// ########### 核心方法 - 列目录（List）- 按parent_id查询子节点 ###########
func (d *StrmList) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if d.db == nil {
		return nil, fmt.Errorf("数据库未就绪")
	}

	// 获取当前目录的nodeID（用于查询子节点）
	dirID, _, _, err := d.findNodeByPath(dir.GetPath())
	if err != nil {
		return nil, fmt.Errorf("获取目录节点ID失败: %v", err)
	}

	// 查询当前目录下的所有子节点（名称/是否目录/内容长度）
	rows, err := d.db.Query(`
		SELECT name, is_dir, length(content) 
		FROM nodes 
		WHERE parent_id = ?
	`, dirID)
	if err != nil {
		return nil, fmt.Errorf("查询子节点失败: %v", err)
	}
	defer rows.Close()

	var objs []model.Obj
	parentPath := strings.TrimSuffix(dir.GetPath(), "/")
	now := time.Now()

	// 遍历查询结果，封装为model.Obj
	for rows.Next() {
		var name string
		var isDir bool
		var contentLen int64
		if err := rows.Scan(&name, &isDir, &contentLen); err != nil {
			log.Warnf("扫描节点失败: %v，跳过该节点", err)
			continue
		}

		// 拼接子节点完整路径（保证/开头的绝对路径）
		childPath := fmt.Sprintf("%s/%s", parentPath, name)
		if !strings.HasPrefix(childPath, "/") {
			childPath = "/" + childPath
		}

		// 封装为框架原生model.Object（无扩展字段，避免编译报错）
		objs = append(objs, &model.Object{
			Name:     name,
			Size:     contentLen,
			Modified: now,
			IsFolder: isDir,
			Path:     childPath,
		})
	}

	// 检查行扫描全局错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("行扫描失败: %v", err)
	}

	log.Debugf("列目录成功：%s，共%d个节点", dir.GetPath(), len(objs))
	return objs, nil
}

// ########### 核心方法 - 获取文件/目录详情（Get） ###########
func (d *StrmList) Get(ctx context.Context, path string) (model.Obj, error) {
	if d.db == nil {
		return nil, fmt.Errorf("数据库未就绪")
	}

	// 查询节点完整信息
	_, isDir, content, err := d.findNodeByPath(path)
	if err != nil {
		return nil, fmt.Errorf("查询节点失败: %v", err)
	}

	// 封装为框架原生model.Object
	obj := &model.Object{
		Name:     filepath.Base(path),
		Size:     int64(len(content)),
		Modified: time.Now(),
		IsFolder: isDir,
		Path:     path,
	}

	log.Debugf("获取节点详情成功：%s，是否目录：%t", path, isDir)
	return obj, nil
}

// ########### 核心方法 - 生成播放链接（Link）- 完全对齐Local驱动 ###########
func (d *StrmList) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if d.db == nil {
		return nil, fmt.Errorf("数据库未就绪")
	}

	// 获取文件完整路径
	filePath := file.GetPath()
	// 初始化框架原生model.Link（无扩展字段）
	link := &model.Link{}
	var mFile model.File

	// 查询strm文件对应的播放URL
	_, _, playURL, err := d.findNodeByPath(filePath)
	if err != nil {
		return nil, fmt.Errorf("获取播放URL失败: %v", err)
	}

	// 将URL包装为model.File（bytes.Reader实现model.File接口，与Local驱动兼容）
	mFile = bytes.NewReader([]byte(playURL))
	// 设置内容长度（从文件对象获取，保证精准）
	link.ContentLength = file.GetSize()

	// 框架核心 - 对齐Local驱动，支持断点续传/流式传输
	link.SyncClosers.AddIfCloser(mFile)
	link.RangeReader = stream.GetRangeReaderFromMFile(link.ContentLength, mFile)
	link.RequireReference = link.SyncClosers.Length() > 0

	// 配置strm文件标准响应头
	link.Header = http.Header{
		"Content-Type":        []string{"text/plain; charset=utf-8"},
		"Content-Disposition": []string{fmt.Sprintf("inline; filename=%s", file.GetName())},
		"X-Content-Type-Options": []string{"nosniff"},
	}

	log.Debugf("生成播放链接成功：%s，URL长度：%d", filePath, len(playURL))
	return link, nil
}

// ########### 内部辅助方法 - 路径转节点ID（逐级解析，核心路径逻辑） ###########
func (d *StrmList) findNodeByPath(path string) (nodeID int64, isDir bool, content string, err error) {
	path = strings.TrimSpace(strings.TrimPrefix(path, "/"))
	// 根路径直接返回固定ID=0
	if path == "" {
		return 0, true, "", nil
	}

	// 拆分路径为分段（如/电影/科幻 → ["电影", "科幻"]）
	pathParts := strings.Split(path, "/")
	var currentParentID int64 = 0 // 从根节点（ID=0）开始遍历

	// 逐级查询每个路径段的节点ID
	for _, part := range pathParts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		// 查询当前分段的节点信息
		err = d.db.QueryRow(`
			SELECT id, is_dir, content 
			FROM nodes 
			WHERE parent_id = ? AND name = ?
		`, currentParentID, part).Scan(&nodeID, &isDir, &content)
		if err != nil {
			return 0, false, "", fmt.Errorf("路径段[%s]不存在: %v", part, err)
		}

		// 更新父节点ID为当前节点ID，继续遍历下一段
		currentParentID = nodeID
	}

	return nodeID, isDir, content, nil
}

// ########### 异步任务 - 导入strm.txt文件到SQLite ###########
func (d *StrmList) importStrmTask() {
	log.Infof("开始导入strm.txt：%s", d.TxtPath)

	// 校验strm.txt文件是否存在
	if _, err := os.Stat(d.TxtPath); os.IsNotExist(err) {
		log.Errorf("strm.txt文件不存在: %s", d.TxtPath)
		return
	}

	// 打开strm.txt文件
	file, err := os.Open(d.TxtPath)
	if err != nil {
		log.Errorf("打开strm.txt失败: %v", err)
		return
	}
	defer file.Close()

	// 开启数据库事务，提升批量导入速度
	tx, err := d.db.Begin()
	if err != nil {
		log.Errorf("开启导入事务失败: %v", err)
		return
	}

	// 初始化根节点（ID=0，避免根节点不存在）
	_, _ = tx.Exec("INSERT OR IGNORE INTO nodes (id, name, parent_id, is_dir) VALUES (0, '', -1, 1)")

	// 预编译插入语句（批量导入必用，提升性能）
	insertNodeStmt, err := tx.Prepare(`
		INSERT INTO nodes (name, parent_id, is_dir, content) 
		VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		log.Errorf("预编译插入语句失败: %v", err)
		_ = tx.Rollback()
		return
	}
	defer insertNodeStmt.Close()

	// 目录缓存 - 避免重复创建相同目录，key：目录路径（如电影/科幻），value：节点ID
	dirCache := map[string]int64{"": 0}
	// 大缓冲区 - 支持超长行的strm文件
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024)

	var successCount, skipCount int // 统计导入结果

	// 逐行解析strm.txt
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// 跳过空行
		if line == "" {
			skipCount++
			continue
		}

		// 按#分割路径和播放URL（严格校验格式：路径#URL）
		parts := strings.SplitN(line, "#", 2)
		if len(parts) < 2 || strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
			log.Warnf("行格式错误，跳过: %s", line)
			skipCount++
			continue
		}

		// 解析路径和URL（去除首尾空格）
		rawPath := strings.TrimSpace(parts[0])
		playURL := strings.TrimSpace(parts[1])
		// 拆分文件路径为目录段和文件名（如电影/科幻/流浪地球.strm → 目录段[电影,科幻]，文件名流浪地球.strm）
		pathParts := strings.Split(rawPath, "/")
		if len(pathParts) < 1 {
			skipCount++
			continue
		}
		filename := strings.TrimSpace(pathParts[len(pathParts)-1])
		dirParts := pathParts[:len(pathParts)-1]

		// 逐级创建目录并更新缓存
		var currParentID int64 = 0
		var currDirPath string // 当前目录的缓存key（如电影/科幻）
		for _, dirPart := range dirParts {
			dirPart = strings.TrimSpace(dirPart)
			if dirPart == "" {
				continue
			}
			// 拼接当前目录的缓存key
			if currDirPath == "" {
				currDirPath = dirPart
			} else {
				currDirPath += "/" + dirPart
			}

			// 缓存命中，直接使用目录ID
			if dirID, ok := dirCache[currDirPath]; ok {
				currParentID = dirID
				continue
			}

			// 缓存未命中，插入目录节点
			res, err := tx.Exec("INSERT INTO nodes (name, parent_id, is_dir) VALUES (?, ?, 1)", dirPart, currParentID)
			if err != nil {
				log.Warnf("创建目录[%s]失败: %v，跳过该文件", currDirPath, err)
				currParentID = 0
				break
			}

			// 获取新目录的ID并加入缓存
			dirID, err := res.LastInsertId()
			if err != nil {
				log.Warnf("获取目录ID失败: %v，跳过该文件", err)
				currParentID = 0
				break
			}
			currParentID = dirID
			dirCache[currDirPath] = dirID
		}

		// 目录创建失败，跳过当前文件
		if currParentID == 0 && filename != "" {
			skipCount++
			continue
		}

		// 插入文件节点（is_dir=0，content=播放URL）
		if _, err := insertNodeStmt.Exec(filename, currParentID, 0, playURL); err == nil {
			successCount++
		} else {
			log.Warnf("插入文件[%s]失败: %v", rawPath, err)
			skipCount++
		}
	}

	// 检查文件扫描错误
	if err := scanner.Err(); err != nil {
		log.Errorf("扫描strm.txt失败: %v，回滚事务", err)
		_ = tx.Rollback()
		return
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		log.Errorf("提交导入事务失败: %v，回滚事务", err)
		_ = tx.Rollback()
		return
	}

	// 导入完成，打印统计信息
	log.Infof("strm.txt导入完成！成功导入：%d条，跳过：%d条", successCount, skipCount)
}

// ########### 驱动注册 - 框架标准方式 ###########
func init() {
	op.RegisterDriver(func() driver.Driver {
		return &StrmList{}
	})
}

// ########### 接口断言 - 编译期校验所有接口实现完整性 ###########
var (
	_ driver.Driver   = (*StrmList)(nil) // 校验基础驱动接口
	_ model.IRootPath = (*StrmList)(nil) // 校验根路径接口
)