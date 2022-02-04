/*
 *
 * Copyright 2015 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package main implements a client for Greeter service.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	// pb "google.golang.org/grpc/examples/helloworld/helloworld"
	pb "google.golang.org/grpc/examples/helloworld/pql"
)

const (
	defaultName = "world"
)

var (
	addr = flag.String("addr", "localhost:50051", "the address to connect to")
	// name = flag.String("name", defaultName, "Name to greet")
)

func main() {
	flag.Parse()
	// Set up a connection to the server.
	conn, err := grpc.Dial(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()
	// c := pb.NewGreeterClient(conn)
	c := pb.NewParserClient(conn)

	// Contact the server and print out its response.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	statement := "PREDICT Survived USING titanic_model GIVEN SELECT a, b FROM titanic"
	statement = `create model m (
		n numeric encoder (type=dense, fc_size=256, use_bias=true, loss.type=mean_squared_error), 
		b binary decoder (dropout=0.2), 
		s set, 
		t text processor (word_tokenizer='sparse'), 
		c category encoder (cell_type=sample_grid(ARRAY['rnn', 'gru', 'lstm'])) 
	)
	with processor (force_split=true, split_probabilities=ARRAY[0.7, 0.1, 0.2], text => (char_tokenizer='characters', level => (n=3)) ) 
	combiner (type='concat', num_fc_layers=range_int(1,4)) 
	trainer (learning_rate=range_real(0.001, 0.1, 4, linear)) 
	hyperopt (goal='minimize', metric='loss', split='validation') 
	target b 
	from ds`
	// statement = "CREATE MODEL m CONFIG '{ \"x\": 1 }' FROM ds"

	r, err := c.Parse(ctx, &pb.ParseRequest{Statement: statement, TargetDialect: pb.ParseRequest_SNOWFLAKE})
	// r, err := c.SayHello(ctx, &pb.HelloRequest{Name: *name})
	if err != nil {
		log.Fatalf("could not greet: %v", err)
	}
	// log.Printf("Greeting: %s", r.GetMessage())
	log.Printf("Parsed SQL: %s", r.GetParsedSql())

	// See if we can parse the model, and get either the String value, or the dictionary value
	// TODO: convert dictionary value to JSON
	model := r.GetClause().GetCreateModel()
	if model.Config != nil && model.Config.GetTypeUrl() == "type.googleapis.com/pql.ModelConfig" {
		config := &pb.ModelConfig{}
		if err := model.Config.UnmarshalTo(config); err != nil {
			log.Fatal(err)
		}
		// m := protojson.MarshalOptions{
		// 	Indent:          "  ",
		// 	EmitUnpopulated: true,
		// 	Resolver: ,
		// }
		// b, err := m.Marshal(config)
		b, err := json.Marshal(config)
		if err != nil {
			fmt.Println(err)
			return
		}
		// TODO: Need to move RANGE and ARRAY values into hyperopt section
		log.Printf("Create Model as String: %v", strings.ToLower(string(b)))
	} else {
		log.Printf("Create Model as String: %v", string(model.Config.GetValue()))
	}

	// TODO: Get back the model config
}
