package main

import (
	"./JsonMessage"
	"./logger"
	"./src"
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
	"time"
)

func main() {

	//create logger
	logger.CreateLogger("logfile")
	defer logger.DropLogger()

	//connect to RabbitMQ
	rabbitmq := "amqp://" + config.RABBITMQ_USER + ":" + config.RABBITMQ_PASS + "@" + config.RABBITMQ_HOST + ":" + config.RABBITMQ_PORT
	conn, err := amqp.Dial(rabbitmq)

	logger.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	logger.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	msgs, err := ch.Consume(
		"analysis", // queue
		"",         // consumer   	consumer에 대한 식별자를 지정합니다. consumer tag는 로컬에 channel이므로, 두 클라이언트는 동일한 consumer tag를 사용할 수있다.
		false,      // autoAck    	false는 명시적 Ack를 해줘야 메시지가 삭제되고 true는 메시지를 빼면 바로 삭제
		false,      // exclusive	현재 connection에만 액세스 할 수 있으며, 연결이 종료 할 때 Queue가 삭제됩니다.
		false,      // noLocal    	필드가 설정되는 경우 서버는이를 published 연결로 메시지를 전송하지 않을 것입니다.
		false,      // noWait		설정하면, 서버는 Method에 응답하지 않습니다. 클라이언트는 응답 Method를 기다릴 것이다. 서버가 Method를 완료 할 수 없을 경우는 채널 또는 연결 예외를 발생시킬 것입니다.
		nil,        // arguments	일부 브로커를 사용하여 메시지의 TTL과 같은 추가 기능을 구현하기 위해 사용된다.
	)
	logger.FailOnError(err, "Failed to register a consumer")

	mariadb := config.MARIADB_USER_PASS + config.MARIADB_PROTOCOL + config.MARIADB_HOST_PORT + config.MARIADB_DB

	// Open database connection
	db, err := sql.Open("mysql", mariadb)
	if err != nil {
		panic(err.Error()) // Just for example purpose. You should use proper error handling instead of panic
	}
	defer db.Close()
	forever := make(chan bool)

	for i := 0; i < 20; i++ {
		go func() {

			for d := range msgs {
				//Decoding arbitrary data
				var m JsonMessage.JsonMessage
				{
					r := json.Unmarshal([]byte(d.Body), &m)
					if err != nil {
						logger.FailOnError(r, "Failed to json.Unmarshal")
						continue
					}
				}

				Tx, _ := db.Begin()
				rows, err := Tx.Prepare("INSERT INTO Requestlog (applicationid, platform, apitype, method, url, reqdate, reqtime, host) VALUES (?, ?, ?, ?, ?, ?, ?,?)")
				if err != nil {
					panic(err.Error()) // proper error handling instead of panic in your app
				}

				var apitype string = m.Api["id"]
				var platform string = m.Api["type"]
				var applicationid string = m.ApplicationId
				var url string = m.Url
				var method string = m.Method
				var host string = m.Host
				t := time.Unix((m.TimeStamp / 1000), 0)
				var reqdate string = fmt.Sprintf("%04d-%02d-%02d", t.Year(), t.Month(), t.Day())
				var reqtime string = fmt.Sprintf("%02d:%02d", t.Hour(), t.Minute())

				//fmt.Println("applicationid :", applicationid, "platform : ", platform, "apitype : ", apitype, "method : ", method, url, reqdate, reqtime, host)
				_, Execerr := rows.Exec(&applicationid, &platform, &apitype, &method, &url, &reqdate, &reqtime, &host)
				if Execerr != nil {
					logger.FailOnError(Execerr, "Failed to Execerr")
				}

				commiterr := Tx.Commit()
				if commiterr != nil {
					logger.FailOnError(commiterr, "Failed to commiterr")
				}
				//RabbitMQ Message delete
				d.Ack(false)
			}

		}()
	}

	<-forever

}
