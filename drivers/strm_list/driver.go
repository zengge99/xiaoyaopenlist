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

	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/OpenListTeam/OpenList/v4/internal/stream"
	log "github.com/sirupsen/logrus"
)

// 驱动配置项 - 保留原核心配置
type Addition struct {
	TxtPath string `json:"txt_path" config:"strm.txt 路径" default:"/index/strm/strm.txt" help:"strm.txt 的绝对路径"`
	DbPath  string `json:"db_path" config:"数据库存放路径" default:"/opt/alist/strm/strm.db" help:"SQLite 数据库文件的绝对路径"`
}

// 驱动配置元数据 - 框架要求的基础配置
var config = driver.Config{
	Name:        "StrmList",
	LocalSort:   true,
	NoCache:     false,
	DefaultRoot: "/",
}

// 驱动核心结构体 - 必须嵌入model.Storage，满足GetStorage指针返回要求
type StrmList struct {
	model.Storage
	Addition
	db *sql.DB
}

// 实现驱动接口：返回驱动配置
func (d *StrmList) Config() driver.Config {
	return config
}

// 实现驱动接口：返回自定义配置项
func (d *StrmList) GetAddition() driver.Additional {
	return &d.Addition
}

// 核心修正：GetStorage返回*model.Storage指针，完全匹配框架接口
func (d *StrmList) GetStorage() *model.Storage {
	return &d.Storage
}

// 驱动初始化 - 保留原逻辑，无修改
func (d *StrmList) Init(ctx context.Context) error {
	if d.DbPath == "" || d.TxtPath == "" {
		return nil
	}
	// 确保数据库目录存在
	if err := os.MkdirAll(filepath.Dir(d.DbPath), 0755); err != nil {
		return fmt.Errorf("创建DB目录失败: %v", err)
	}
	// 打开SQLite数据库（内置驱动+性能参数）
	var err error
	d.db, err = sql.Open("sqlite", d.DbPath+"?_pragma=journal_mode(OFF)&_pragma=synchronous(OFF)")
	if err != nil {
		return fmt.Errorf("打开SQLite失败: %v", err)
	}
	// 初始化表结构和索引
	_, err = d.db.Exec(`
		CREATE TABLE IF NOT EXISTS nodes (
			id INTEGER PRIMARY KEY,
			name TEXT,
			parent_id INTEGER,
			is_dir BOOLEAN,
			content TEXT
		);
		CREATE INDEX IF NOT EXISTS idx_parent_name ON nodes(parent_id, name);
	`)
	if err != nil {
		return fmt.Errorf("初始化表结构失败: %v", err)
	}
	// 空库时异步导入strm.txt
	var count int
	_ = d.db.QueryRow("SELECT COUNT(*) FROM nodes").Scan(&count)
	if count <= 1 {
		go d.importTxtTask()
	}
	return nil
}

// 驱动销毁：释放DB连接
func (d *StrmList) Drop(ctx context.Context) error {
	if d.db != nil {
		return d.db.Close()
	}
	return nil
}

// 核心：列目录 - 修复nodeID未使用，恢复原查询逻辑
func (d *StrmList) List(ctx context.Context, dir model.Obj, args model.ListArgs) ([]model.Obj, error) {
	if d.db == nil {
		return nil, fmt.Errorf("strm_list: 数据库未就绪")
	}
	// 获取当前目录节点ID（用于查询子节点，解决未使用问题）
	nodeID, _, _, err := d.findNodeByPath(dir.GetPath())
	if err != nil {
		return nil, fmt.Errorf("获取目录节点失败: %v", err)
	}
	// 基于parent_id查询子节点，和findNodeByPath路径解析一致
	rows, err := d.db.Query(
		"SELECT name, is_dir, length(content) FROM nodes WHERE parent_id = ?",
		nodeID,
	)
	if err != nil {
		return nil, fmt.Errorf("查询子节点失败: %v", err)
	}
	defer rows.Close()

	var objs []model.Obj
	now := time.Now()
	fullParentPath := strings.TrimSuffix(dir.GetPath(), "/")

	for rows.Next() {
		var name string
		var isDir bool
		var contentLen int64
		if err := rows.Scan(&name, &isDir, &contentLen); err != nil {
			log.Warnf("[StrmList] 扫描节点失败: %v", err)
			continue
		}
		// 拼接标准完整路径
		fullPath := fmt.Sprintf("%s/%s", fullParentPath, name)
		if !strings.HasPrefix(fullPath, "/") {
			fullPath = "/" + fullPath
		}
		// 仅使用框架原生model.Object字段
		obj := &model.Object{
			Name:     name,
			Size:     contentLen,
			Modified: now,
			IsFolder: isDir,
			Path:     fullPath,
		}
		objs = append(objs, obj)
	}
	// 检查行扫描全局错误
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("行扫描失败: %v", err)
	}
	return objs, nil
}

// 核心：获取文件/目录详情 - 仅使用原生字段
func (d *StrmList) Get(ctx context.Context, path string) (model.Obj, error) {
	if d.db == nil {
		return nil, fmt.Errorf("strm_list: 数据库未就绪")
	}
	// 查询节点完整信息
	_, isDir, content, err := d.findNodeByPath(path)
	if err != nil {
		return nil, fmt.Errorf("查询节点失败: %v", err)
	}
	// 纯原生model.Object字段
	return &model.Object{
		Name:     filepath.Base(path),
		Size:     int64(len(content)),
		Modified: time.Now(),
		IsFolder: isDir,
		Path:     path,
	}, nil
}

// 核心：Link方法 - 完全对齐Local驱动，框架原生实现，无编译报错
func (d *StrmList) Link(ctx context.Context, file model.Obj, args model.LinkArgs) (*model.Link, error) {
	if d.db == nil {
		return nil, fmt.Errorf("strm_list: 数据库未就绪")
	}
	// 获取文件完整路径
	fullPath := file.GetPath()
	// 初始化原生model.Link，和Local驱动一致
	link := &model.Link{}
	var MFile model.File

	// 查询strm文件实际内容（URL）
	_, _, content, err := d.findNodeByPath(fullPath)
	if err != nil {
		return nil, fmt.Errorf("获取strm内容失败: %v", err)
	}
	// 将内容包装为框架原生支持的model.File（和Local驱动的os.File接口兼容）
	MFile = bytes.NewReader([]byte(content))
	// 设置ContentLength，和Local驱动一致从file.GetSize()获取
	link.ContentLength = file.GetSize()

	// 框架核心：和Local驱动完全一致的写法，支持断点续传/流式传输
	link.SyncClosers.AddIfCloser(MFile)
	link.RangeReader = stream.GetRangeReaderFromMFile(link.ContentLength, MFile)
	link.RequireReference = link.SyncClosers.Length() > 0

	// 配置strm标准响应头，对齐Local驱动风格
	link.Header = http.Header{
		"Content-Type":                []string{"text/plain; charset=utf-8"},
		"Content-Disposition":         []string{fmt.Sprintf("inline; filename=%s", file.GetName())},
		"X-Content-Type-Options":      []string{"nosniff"},
	}

	return link, nil
}

// 内部辅助：路径转节点ID - 逐级解析，核心逻辑无修改
func (d *StrmList) findNodeByPath(path string) (id int64, isDir bool, content string, err error) {
	path = strings.Trim(path, "/")
	if path == "" {
		return 0, true, "", nil // 根目录固定ID=0
	}
	parts := strings.Split(path, "/")
	var currentParent int64 = 0
	for _, part := range parts {
		err = d.db.QueryRow(
			"SELECT id, is_dir, content FROM nodes WHERE parent_id = ? AND name = ?",
			currentParent, part,
		).Scan(&id, &isDir, &content)
		if err != nil {
			return 0, false, "", fmt.Errorf("路径段[%s]查询失败: %v", part, err)
		}
		currentParent = id
	}
	return id, isDir, content, nil
}

// 异步导入strm.txt - 保留原高效逻辑，事务+预编译+目录缓存
func (d *StrmList) importTxtTask() {
	log.Infof("[StrmList] 开始从 %s 导入数据", d.TxtPath)
	file, err := os.Open(d.TxtPath)
	if err != nil {
		log.Errorf("[StrmList] 打开strm.txt失败: %v", err)
		return
	}
	defer file.Close()

	// 事务提升导入效率
	tx, err := d.db.Begin()
	if err != nil {
		log.Errorf("[StrmList] 开启事务失败: %v", err)
		return
	}
	// 初始化根节点
	_, _ = tx.Exec("INSERT OR IGNORE INTO nodes (id, name, parent_id, is_dir) VALUES (0, '', -1, 1)")
	// 预编译插入语句
	stmt, err := tx.Prepare("INSERT INTO nodes (name, parent_id, is_dir, content) VALUES (?, ?, ?, ?)")
	if err != nil {
		log.Errorf("[StrmList] 预编译语句失败: %v", err)
		_ = tx.Rollback()
		return
	}
	defer stmt.Close()

	dirCache := map[string]int64{"": 0} // 目录缓存避免重复创建
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 10*1024*1024) // 大缓冲区支持超长行

	count := 0
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		// 按#分割路径和内容，格式：路径#strm_URL
		parts := strings.SplitN(line, "#", 2)
		if len(parts) < 2 || parts[0] == "" || parts[1] == "" {
			continue
		}
		rawPath, content := strings.Trim(parts[0], "/"), parts[1]
		pathParts := strings.Split(rawPath, "/")
		if len(pathParts) == 0 {
			continue
		}

		// 逐级创建目录并缓存
		var currParent int64 = 0
		currPathAcc := ""
		for _, part := range pathParts[:len(pathParts)-1] {
			if currPathAcc == "" {
				currPathAcc = part
			} else {
				currPathAcc += "/" + part
			}
			if id, ok := dirCache[currPathAcc]; ok {
				currParent = id
				continue
			}
			// 插入目录节点
			res, err := tx.Exec("INSERT INTO nodes (name, parent_id, is_dir) VALUES (?, ?, 1)", part, currParent)
			if err == nil {
				currParent, _ = res.LastInsertId()
				dirCache[currPathAcc] = currParent
			}
		}
		// 插入strm文件节点
		if _, err := stmt.Exec(pathParts[len(pathParts)-1], currParent, 0, content); err == nil {
			count++
			if count%100000 == 0 {
				log.Infof("[StrmList] 导入中：已处理 %d 条", count)
			}
		}
	}

	// 检查扫描错误并提交事务
	if err := scanner.Err(); err != nil {
		log.Errorf("[StrmList] 扫描文件失败: %v", err)
		_ = tx.Rollback()
		return
	}
	if err := tx.Commit(); err != nil {
		log.Errorf("[StrmList] 提交事务失败: %v", err)
		return
	}
	log.Infof("[StrmList] 导入完成，有效记录: %d 条", count)
}

// 驱动注册：框架标准方式
func init() {
	op.RegisterDriver(func() driver.Driver {
		return &StrmList{}
	})
}

// 接口断言：编译期验证实现完整性
var _ driver.Driver = (*StrmList)(nil)