package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

func main() {
	// 定义命令行参数
	addr := flag.String("addr", "localhost:6379", "Redis server address")
	password := flag.String("password", "", "Redis password (if any)")
	db := flag.Int("db", 0, "Redis database number")
	interval := flag.Int("interval", 300, "Delete Interval")

	// 解析命令行参数
	flag.Parse()

	// 创建 Redis 客户端
	rdb := redis.NewClient(&redis.Options{
		Addr:     *addr,     // Redis 地址
		Password: *password, // Redis 密码
		DB:       *db,       // Redis 数据库
	})

	ctx := context.Background()

	// 检查当前 notify-keyspace-events 配置
	currentConfig, err := rdb.ConfigGet(ctx, "notify-keyspace-events").Result()
	if err != nil {
		log.Fatalf("Failed to get configuration: %v", err)
	}

	// currentConfig[1] 是 interface{} 类型，我们需要类型断言为 string
	if len(currentConfig) < 2 {
		log.Fatal("Failed to get notify-keyspace-events configuration")
	}

	configValue, ok := currentConfig[1].(string)
	if !ok {
		log.Fatal("Failed to convert config value to string")
	}

	// 判断是否已经配置过期通知 (检查 "E" 或 "x" 是否在配置字符串中)
	log.Println("Configured notify-keyspace-events")
	if !(strings.Contains(configValue, "E") || strings.Contains(configValue, "x")) {
		_, err := rdb.ConfigSet(ctx, "notify-keyspace-events", "Ex").Result()
		if err != nil {
			log.Fatalf("Failed to set configuration: %v", err)
		}
		log.Println("Configured notify-keyspace-events to 'Ex'")
	} else {
		log.Println("notify-keyspace-events is already configured to support expiration notifications")
	}

	// 订阅过期事件频道
	pubsub := rdb.PSubscribe(ctx, "__keyevent@0__:expired")
	defer pubsub.Close()

	// 检查订阅是否成功
	_, err = pubsub.Receive(ctx)
	if err != nil {
		log.Fatalf("Failed to subscribe to the channel: %v", err)
	}

	// 存储过期键的文件路径
	expiredFilePath := ".expired_keys"

	// 启动一个 goroutine 来处理过期事件
	go func() {
		for msg := range pubsub.Channel() {
			log.Printf("Receive Key expired: %s\n", msg.Payload) // 打印过期的键名

			// 记录过期键到文件
			err := appendExpiredKeyToFile(expiredFilePath, msg.Payload)
			if err != nil {
				log.Fatalf("Failed to write expired key to file: %v", err)
			}
		}
	}()

	// 启动定时任务，在每天午夜执行惰性删除
	startDailyCleanup(rdb, expiredFilePath, *interval)

	// // 使用无限循环保持程序持续运行
	// for {
	// 	// 可以在这里添加一些其他的逻辑，或者让程序什么都不做，保持运行
	// 	time.Sleep(1 * time.Second) // 暂时休眠，防止 CPU 占用过高
	// }
}

// 将过期键追加到文件中
func appendExpiredKeyToFile(filePath, key string) error {
	// 打开文件，如果文件不存在则创建
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// 将过期键写入文件
	_, err = file.WriteString(key + "\n")
	return err
}

// 每天零点执行惰性删除
func startDailyCleanup(rdb *redis.Client, filePath string, interval int) {
	// 设置每天午夜 0 点执行任务
	ticker := time.NewTicker(24 * time.Hour)

	// 等待直到每天的 0 点
	now := time.Now()
	waitUntilMidnight := time.Until(time.Date(now.Year(), now.Month(), now.Day()+1, 0, 0, 0, 0, now.Location()))
	time.Sleep(waitUntilMidnight)

	for {
		// 在零点执行清理
		err := performLazyDelete(rdb, filePath, interval)
		if err != nil {
			log.Fatalf("Error during lazy deletion: %v", err)
		}

		// 等待下次零点
		<-ticker.C
	}
}

// 执行惰性删除操作
func performLazyDelete(rdb *redis.Client, filePath string, interval int) error {
	log.Println("Start lazily deleting")

	backupFilePath := filePath + ".bak"
	err := copyFile(filePath, backupFilePath)
	if err != nil {
		return fmt.Errorf("failed to backup file: %v", err)
	}
	err = os.Truncate(filePath, 0)
	if err != nil {
		return err
	}
	// 读取存储的过期键
	file, err := os.Open(backupFilePath)
	if err != nil {
		return err
	}
	defer file.Close()

	var keysToCheck []string
	// 读取每一行（即过期键）
	var key string
	for {
		_, err := fmt.Fscanf(file, "%s\n", &key)
		if err != nil {
			break
		}
		keysToCheck = append(keysToCheck, key)
	}

	// 执行惰性删除操作（访问键以触发过期删除）
	for _, key := range keysToCheck {
		// 获取键的类型
		_, err := rdb.Type(context.Background(), key).Result()
		if err != nil {
			log.Fatalf("Failed to get type of key %s: %v\n", key, err)
			continue
		} else {
			log.Printf("get type of key %s\n", key)
		}

		time.Sleep(time.Duration(interval) * time.Millisecond)
	}

	// 删除备份文件
	err = os.Remove(backupFilePath)

	return err
}

func copyFile(srcPath, destPath string) error {
	// 打开源文件
	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// 创建目标文件
	destFile, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer destFile.Close()

	// 使用一个 map 来去重
	seen := make(map[string]struct{})

	// 使用 bufio.Scanner 逐行读取源文件
	scanner := bufio.NewScanner(srcFile)
	for scanner.Scan() {
		line := scanner.Text()

		// 如果这行内容没有出现过，则写入目标文件
		if _, ok := seen[line]; !ok {
			seen[line] = struct{}{}
			_, err := destFile.WriteString(line + "\n")
			if err != nil {
				return err
			}
		}
	}

	// 检查扫描时是否遇到错误
	if err := scanner.Err(); err != nil {
		return err
	}

	return nil
}
