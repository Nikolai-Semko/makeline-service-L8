package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/Azure/go-amqp"
)

func getOrdersFromQueue() ([]Order, error) {
	ctx := context.Background()

	var orders []Order

	// Get queue name from environment variable
	orderQueueName := os.Getenv("ORDER_QUEUE_NAME")
	if orderQueueName == "" {
		orderQueueName = os.Getenv("SERVICEBUS_QUEUE_NAME")
		if orderQueueName == "" {
			log.Printf("ORDER_QUEUE_NAME or SERVICEBUS_QUEUE_NAME is not set")
			return nil, errors.New("order queue name is not set")
		}
	}

	// Check if Azure Service Bus connection string is set (primary method)
	serviceBusConnectionString := os.Getenv("SERVICEBUS_CONNECTION_STRING")
	if serviceBusConnectionString != "" {
		log.Printf("Using Azure Service Bus with connection string")
		
		// Create a Service Bus client using the connection string
		client, err := azservicebus.NewClientFromConnectionString(serviceBusConnectionString, nil)
		if err != nil {
			log.Printf("failed to create Service Bus client: %v", err)
			return nil, err
		}
		
		// Create a receiver for the queue
		receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
		if err != nil {
			log.Printf("failed to create receiver: %v", err)
			client.Close(context.TODO())
			return nil, err
		}
		defer receiver.Close(context.TODO())
		
		// Receive messages from the queue with a 5-second timeout
		receiveCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		
		messages, err := receiver.ReceiveMessages(receiveCtx, 10, nil)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			log.Printf("failed to receive messages: %v", err)
			return nil, err
		}
		
		log.Printf("Received %d messages from Azure Service Bus queue", len(messages))
		
		for _, message := range messages {
			log.Printf("message received: %s\n", string(message.Body))
			
			// Parse message body into Order
			order, err := unmarshalOrderFromQueue(message.Body)
			if err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				continue // Skip this message but continue processing others
			}
			
			// Add order to []order slice
			orders = append(orders, order)
			
			// Complete the message (acknowledge receipt)
			err = receiver.CompleteMessage(context.TODO(), message, nil)
			if err != nil {
				log.Printf("failed to complete message: %v", err)
			}
		}
		
		return orders, nil
	}

	// Check if USE_WORKLOAD_IDENTITY_AUTH is set (secondary method)
	useWorkloadIdentityAuth := os.Getenv("USE_WORKLOAD_IDENTITY_AUTH")
	if useWorkloadIdentityAuth == "" {
		useWorkloadIdentityAuth = "false"
	}

	orderQueueHostName := os.Getenv("AZURE_SERVICEBUS_FULLYQUALIFIEDNAMESPACE")
	if orderQueueHostName == "" {
		orderQueueHostName = os.Getenv("ORDER_QUEUE_HOSTNAME")
	}

	if orderQueueHostName != "" && useWorkloadIdentityAuth == "true" {
		cred, err := azidentity.NewDefaultAzureCredential(nil)
		if err != nil {
			log.Fatalf("failed to obtain a workload identity credential: %v", err)
		}

		client, err := azservicebus.NewClient(orderQueueHostName, cred, nil)
		if err != nil {
			log.Fatalf("failed to obtain a service bus client with workload identity credential: %v", err)
		} else {
			fmt.Println("successfully created a service bus client with workload identity credentials")
		}

		receiver, err := client.NewReceiverForQueue(orderQueueName, nil)
		if err != nil {
			log.Fatalf("failed to create receiver: %v", err)
		}
		defer receiver.Close(context.TODO())

		messages, err := receiver.ReceiveMessages(context.TODO(), 10, nil)
		if err != nil {
			log.Fatalf("failed to receive messages: %v", err)
		}

		for _, message := range messages {
			log.Printf("message received: %s\n", string(message.Body))

			// First, unmarshal the JSON data into a string
			var jsonStr string
			err = json.Unmarshal(message.Body, &jsonStr)
			if err != nil {
				log.Printf("failed to deserialize message: %s", err)
				return nil, err
			}

			// Then, unmarshal the string into an Order
			order, err := unmarshalOrderFromQueue([]byte(jsonStr))
			if err != nil {
				log.Printf("failed to unmarshal message: %v", err)
				return nil, err
			}

			// Add order to []order slice
			orders = append(orders, order)

			err = receiver.CompleteMessage(context.TODO(), message, nil)
			if err != nil {
				log.Fatalf("failed to complete message: %v", err)
			}
		}
	} else {
		// Fallback to RabbitMQ/AMQP (legacy method)
		// Get order queue connection string from environment variable
		orderQueueUri := os.Getenv("ORDER_QUEUE_URI")
		if orderQueueUri == "" {
			log.Printf("ORDER_QUEUE_URI is not set")
			return nil, errors.New("ORDER_QUEUE_URI is not set")
		}

		// Get queue username from environment variable
		orderQueueUsername := os.Getenv("ORDER_QUEUE_USERNAME")
		if orderQueueName == "" {
			log.Printf("ORDER_QUEUE_USERNAME is not set")
			return nil, errors.New("ORDER_QUEUE_USERNAME is not set")
		}

		// Get queue password from environment variable
		orderQueuePassword := os.Getenv("ORDER_QUEUE_PASSWORD")
		if orderQueuePassword == "" {
			log.Printf("ORDER_QUEUE_PASSWORD is not set")
			return nil, errors.New("ORDER_QUEUE_PASSWORD is not set")
		}

		// Connect to order queue
		conn, err := amqp.Dial(ctx, orderQueueUri, &amqp.ConnOptions{
			SASLType: amqp.SASLTypePlain(orderQueueUsername, orderQueuePassword),
		})
		if err != nil {
			log.Printf("%s: %s", "failed to connect to order queue", err)
			return nil, err
		}
		defer conn.Close()

		session, err := conn.NewSession(ctx, nil)
		if err != nil {
			log.Printf("unable to create a new session")
		}

		{
			// create a receiver
			receiver, err := session.NewReceiver(ctx, orderQueueName, nil)
			if err != nil {
				log.Printf("creating receiver link: %s", err)
				return nil, err
			}
			defer func() {
				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				receiver.Close(ctx)
				cancel()
			}()

			for {
				log.Printf("getting orders")

				ctx, cancel := context.WithTimeout(ctx, 1*time.Second)
				defer cancel()

				// receive next message
				msg, err := receiver.Receive(ctx, nil)
				if err != nil {
					if err.Error() == "context deadline exceeded" {
						log.Printf("no more orders for you: %v", err.Error())
						break
					} else {
						return nil, err
					}
				}

				messageBody := string(msg.GetData())
				log.Printf("message received: %s\n", messageBody)

				order, err := unmarshalOrderFromQueue(msg.GetData())
				if err != nil {
					log.Printf("failed to unmarshal message: %s", err)
					return nil, err
				}

				// Add order to []order slice
				orders = append(orders, order)

				// accept message
				if err = receiver.AcceptMessage(context.TODO(), msg); err != nil {
					log.Printf("failure accepting message: %s", err)
					// remove the order from the slice so that we pick it up on the next run
					orders = orders[:len(orders)-1]
				}
			}
		}
	}
	return orders, nil
}

func unmarshalOrderFromQueue(data []byte) (Order, error) {
	var order Order

	err := json.Unmarshal(data, &order)
	if err != nil {
		// Try to handle the case where the message is a JSON string containing an order
		var jsonStr string
		jsonErr := json.Unmarshal(data, &jsonStr)
		if jsonErr == nil {
			// If we successfully got a JSON string, try to unmarshal it into an Order
			innerErr := json.Unmarshal([]byte(jsonStr), &order)
			if innerErr == nil {
				return order, nil
			}
			log.Printf("failed to unmarshal inner JSON string: %v\n", innerErr)
		}
		
		log.Printf("failed to unmarshal order: %v\n", err)
		return Order{}, err
	}

	// add orderkey to order
	order.OrderID = strconv.Itoa(rand.Intn(100000))

	// set the status to pending
	order.Status = Pending

	return order, nil
}