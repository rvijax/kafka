/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

/*
 * producer.cpp
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#include <boost/lexical_cast.hpp>

#include "consumer.hpp"

namespace kafkaconnect {

consumer::consumer(boost::asio::io_service& io_service, const error_handler_function& error_handler)
	: _connected(false)
	, _resolver(io_service)
	, _socket(io_service)
	, _error_handler(error_handler)
{
}

consumer::~consumer()
{
	close();
}

void consumer::connect(const std::string& hostname, const uint16_t port)
{
	connect(hostname, boost::lexical_cast<std::string>(port));
}

void consumer::connect(const std::string& hostname, const std::string& servicename)
{
	boost::asio::ip::tcp::resolver::query query(hostname, servicename);
	_resolver.async_resolve(
		query,
		boost::bind(
			&consumer::handle_resolve, this,
			boost::asio::placeholders::error, boost::asio::placeholders::iterator
		)
	);
}

void consumer::close()
{
	_connected = false;
	_socket.close();
}

bool consumer::is_connected() const
{
	return _connected;
}


void consumer::handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		boost::asio::ip::tcp::endpoint endpoint = *endpoints;
		_socket.async_connect(
			endpoint,
			boost::bind(
				&consumer::handle_connect, this,
				boost::asio::placeholders::error, ++endpoints
			)
		);
	}
	else { fail_fast_error_handler(error_code); }
}

void consumer::handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints)
{
	if (!error_code)
	{
		// The connection was successful. Send the request.
		_connected = true;
	}
	else if (endpoints != boost::asio::ip::tcp::resolver::iterator())
	{
		// TODO: handle connection error (we might not need this as we have others though?)

		// The connection failed, but we have more potential endpoints so throw it back to handle resolve
		_socket.close();
		handle_resolve(boost::system::error_code(), endpoints);
	}
	else { fail_fast_error_handler(error_code); }
}

/*
void consumer::handle_write_request_size(const boost::system::error_code& error_code, boost::asio::streambuf* buffer, const std::string& topic, const uint32_t partition)
{
	if (error_code)
	{
		fail_fast_error_handler(error_code);
		std::cout << "error1" << std::endl;
	}

	boost::asio::streambuf* buffer_write_consumer_request = new boost::asio::streambuf();
	std::ostream stream_write_consumer_request(buffer_write_consumer_request);

	// send consume request
	kafkaconnect::encode_consumer_request(stream_write_consumer_request, topic, partition);

	boost::asio::async_write(
		_socket, *buffer_write_consumer_request,
		boost::bind(&consumer::handle_write_request_body, this, boost::asio::placeholders::error, buffer_write_consumer_request)
	);

	delete buffer;
}

void consumer::handle_write_request_body(const boost::system::error_code& error_code, boost::asio::streambuf* buffer)
{
	if (error_code)
	{
		fail_fast_error_handler(error_code);
	}

	delete buffer;

	// TODO: make this more efficient with memory allocations.
	boost::asio::streambuf* buffer_read = new boost::asio::streambuf();

	size_t header;
	boost::asio::read(
		_socket,
		boost::asio::buffer( &header, sizeof(header) )
	);
	header = htonl(header);
	std::cout << "body is " << header << " bytes" << std::endl;

	buffer_read->prepare( header );
	boost::asio::read(
		_socket,
		boost::asio::buffer( buffer_read, header )
	);

	boost::asio::async_read(
			_socket, *buffer_read,
			boost::bind(&consumer::handle_read_request_head, this, boost::asio::placeholders::error, buffer_read)
	);

	buffer_read->commit(header);
	std::istream stream_read(buffer_read);
	//kafkaconnect::decode_consumer(stream_read, messages);

	std::cout << "response received." << std::endl;
}

//template <typename List>
void consumer::handle_read_request(const boost::system::error_code& error_code, boost::asio::streambuf* buffer)
//void consumer::handle_read_request(const boost::system::error_code& error_code)
{
	if (error_code)
	{
		std::cout << "Error" << std::endl;
		fail_fast_error_handler(error_code);
	}
	else
	{

		std::ostringstream ss;
		ss << buf;
		std::string s = ss.str();

		std::cout << "Message received." << std::endl;

		//read data response
		std::istream stream_read(buffer);
		kafkaconnect::decode_consumer(stream_read, messages);

	}

	std::cout << "reached" << std::endl;

	//delete buffer;
}*/

/*void handle_read_header(const boost::system::error_code& error)
{
  if (!error && read_msg_.decode_header())
  {
    boost::asio::async_read(socket_,
        boost::asio::buffer(read_msg_.body(), read_msg_.body_length()),
        boost::bind(&chat_client::handle_read_body, this,
          boost::asio::placeholders::error));
  }
  else
  {
    do_close();
  }
}

void handle_read_body(const boost::system::error_code& error)
{
  if (!error)
  {
    std::cout.write(read_msg_.body(), read_msg_.body_length());
    std::cout << "\n";
    boost::asio::async_read(socket_,
        boost::asio::buffer(read_msg_.data(), chat_message::header_length),
        boost::bind(&chat_client::handle_read_header, this,
          boost::asio::placeholders::error));
  }
  else
  {
    do_close();
  }
}*/


}
