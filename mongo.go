package main

/*数据库为:mongoDB
数据库名 test
表queue1，queue2，queue3为假设的三个匹配队列，同时进行游戏匹配
user为游戏玩家用户表
queue表为类似消息队列作用
*/
import (
	"container/list"
	"fmt"
	"log"
	"sync"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

//定义常量 N代表一个队列匹配的用户数量（N个玩家匹配在一起游戏）
const N int = 5

//用户
type User struct {
	State int
	Name  string
}

//消息队列
type Que struct {
	Name string
}

//Que1.2.3为三个匹配队列

type Que1 struct {
	Name string
}
type Que2 struct {
	Name string
}
type Que3 struct {
	Name string
}

//用于多线程从消息队列读数据时,特设的线程安全的队列
type Queue struct {
	data *list.List
}

var queues []Que

//var l = list.New()
var l1 = list.New()
var l2 = list.New()
var l3 = list.New()

//实现队列链表线程安全
func NewQueue() *Queue {
	q := new(Queue)
	q.data = list.New()
	return q
}

func (q *Queue) push(v interface{}) {
	defer lock.Unlock()
	lock.Lock()
	q.data.PushFront(v)
}

func (q *Queue) pop() interface{} {
	defer lock.Unlock()
	lock.Lock()
	iter := q.data.Back()
	v := iter.Value
	q.data.Remove(iter)
	return v
}

func (q *Queue) dump() {
	for iter := q.data.Back(); iter != nil; iter = iter.Prev() {
		fmt.Println("item:", iter.Value)
	}
}

var lock sync.Mutex

func que() {
	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	q := session.DB("test").C("user")
	//qo := session.DB("test").C("queue")

	for true {
		result := Que{}
		err = q.Find(bson.M{"state": 0}).One(&result)
		if err != nil {
			break
			log.Fatal(err)
		}
		//l.PushBack(result.Name)
		err = q.Update(
			bson.M{"name": result.Name},
			bson.M{"$set": bson.M{"state": 1}})
		if err != nil {
			log.Fatal(err)
		}

		c := session.DB("test").C("queue")
		err = c.Insert(&Que{result.Name})
		if err != nil {
			log.Fatal(err)
		}
	}
	/*qu := NewQueue()
	for true {

		res := Que{}
		err = c.Find(nil).One(&res)
		if err != nil {
			break
			log.Fatal(err)
		}
		qu.push(res.Name)
	}
	qu.push("hi")
	qu.dump()*/
}
func que1(qu *Queue) {
	for true {

		if qu.data.Back() == nil {
			break
		}
		x := qu.pop().(string)
		//var x = qu.pop().(string)
		//x := err.(string)

		l1.PushBack(x)
		session, err := mgo.Dial("localhost:27017")
		if err != nil {
			panic(err)
		}
		defer session.Close()

		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)
		ss := session.DB("test").C("queue1")
		err = ss.Insert(&Que1{x})

		if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(x)
		if l1.Len() == N {
			var balance [N]string
			for i := 0; i < N; i++ {
				balance[i] = l1.Front().Value.(string)
				//fmt.Printf(l1.Front().Value.(string))
				l1.Remove(l1.Front())
			}
			var str string = "-"
			for i := 0; i < N; i++ {
				str += (balance[i] + "-")
			}

			fmt.Printf("队列1" + str + "\n")
			ss.RemoveAll(nil)
			//l1.Init()
		}
	}
}
func que2(qu *Queue) {
	for true {

		if qu.data.Back() == nil {
			break
		}
		x := qu.pop().(string)
		//var x = qu.pop().(string)
		//x := err.(string)

		l2.PushBack(x)
		session, err := mgo.Dial("localhost:27017")
		if err != nil {
			panic(err)
		}
		defer session.Close()

		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)
		ss := session.DB("test").C("queue2")
		err = ss.Insert(&Que2{x})

		if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(x)
		if l2.Len() == N {
			var balance [N]string
			for i := 0; i < N; i++ {
				balance[i] = l2.Front().Value.(string)
				//fmt.Printf(l1.Front().Value.(string))
				l2.Remove(l2.Front())
			}
			var str string = "-"
			for i := 0; i < N; i++ {
				str += (balance[i] + "-")
			}

			fmt.Printf("队列2" + str + "\n")
			ss.RemoveAll(nil)
			//l1.Init()
		}
	}
}
func que3(qu *Queue) {
	for true {

		if qu.data.Back() == nil {
			break
		}
		x := qu.pop().(string)
		//var x = qu.pop().(string)
		//x := err.(string)

		l3.PushBack(x)
		session, err := mgo.Dial("localhost:27017")
		if err != nil {
			panic(err)
		}
		defer session.Close()

		// Optional. Switch the session to a monotonic behavior.
		session.SetMode(mgo.Monotonic, true)
		ss := session.DB("test").C("queue3")
		err = ss.Insert(&Que3{x})

		if err != nil {
			log.Fatal(err)
		}
		//fmt.Println(x)
		if l3.Len() == N {
			var balance [N]string
			for i := 0; i < N; i++ {
				balance[i] = l3.Front().Value.(string)
				//fmt.Printf(l1.Front().Value.(string))
				l3.Remove(l3.Front())
			}
			var str string = "-"
			for i := 0; i < N; i++ {
				str += (balance[i] + "-")
			}

			fmt.Printf("队列3" + str + "\n")
			ss.RemoveAll(nil)
			//l1.Init()
		}
	}
}
func main() {
	go que() //将user表中 标识(列名)为0的数据找出,改为1,将用户名复制到queue表中
	time.Sleep(1000 * time.Millisecond)
	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	defer session.Close()

	// Optional. Switch the session to a monotonic behavior.
	session.SetMode(mgo.Monotonic, true)
	s := session.DB("test").C("queue")
	err = s.Find(nil).All(&queues)
	if err != nil {
		log.Fatal(err)
	}
	qu := NewQueue()
	for i := 0; i < len(queues); i++ {
		qu.push(queues[i].Name)
	}

	//从消息队列中抓人,匹配到队列里
	go que1(qu)
	go que2(qu)
	go que3(qu)
	time.Sleep(1000 * time.Millisecond)
	fmt.Println("over")

}
