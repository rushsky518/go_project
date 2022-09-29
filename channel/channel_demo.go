package main

import "fmt"

func fibonacci(c, quit chan int) {
	x, y := 0, 1
	for {
		select {
		// 向 channel 中写数据
		case c <- x:
			x, y = y, x+y
		case <-quit:
			fmt.Println("quit")
			return
		}
	}
}

func main() {
	// 创建 int 类型的 channel，阻塞型
	c := make(chan int)
	quit := make(chan int)
	go func() {
		for i := 0; i < 10; i++ {
			// 读取 channel 中的数据
			fmt.Println(<-c)
		}
		quit <- 0
	}()
	fibonacci(c, quit)
}
