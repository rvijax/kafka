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
 *
 *  Created on: 21 Jun 2011
 *      Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_ENCODER_HELPER_HPP_
#define KAFKA_ENCODER_HELPER_HPP_

#include <ostream>
#include <iostream>
#include <istream>
#include <string>

#include <arpa/inet.h>
#include <boost/crc.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

#include <stdint.h>

namespace kafkaconnect {
namespace test { class encoder_consumer_helper; }

const uint16_t kafka_format_version = 1; // FETCh

const uint8_t message_format_magic_number = 0;
const uint8_t message_format_extra_data_size = 1 + 4;
const uint8_t message_format_header_size = message_format_extra_data_size + 4;

const uint32_t max_size = 1024 * 1024;

class encoder_consumer_helper
{
private:
	friend class test::encoder_consumer_helper;
	friend void encode_consumer_request_size(std::ostream& , const std::string& );
	friend void encode_consumer_request(std::ostream&, const std::string&, const uint32_t);
	template <typename List> friend void decode_consumer(char* buffer_read, uint32_t &buffer_read_cursor, const uint32_t buffer_size, List& messages);

	static std::ostream& message_encode(std::ostream& stream, const std::string message)
	{
		// Message format is ... message & data size (4 bytes)
		raw(stream, htonl(message_format_extra_data_size + message.length()));

		// ... magic number (1 byte)
		stream << message_format_magic_number;

		// ... string crc32 (4 bytes)
		boost::crc_32_type result;
		result.process_bytes(message.c_str(), message.length());
		raw(stream, htonl(result.checksum()));

		// ... message string bytes
		stream << message;

		return stream;
	}

	static void message_decode(char* buffer_read, uint32_t &buffer_read_cursor, const uint32_t buffer_size, std::string &message, uint32_t message_size)
	{
/*
    A message. The format of an N byte message is the following:
	 4 message size <-- already parsed in caller.
	 1 byte "magic" identifier to allow format changes
	 4 byte CRC32 of the payload
	 N - 5 byte payload
*/
		// ... magic number (1 byte)
		uint8_t message_format_magic_number;
		raw(buffer_read, buffer_read_cursor, buffer_size, message_format_magic_number, 1);

		// Message format is ... message & data size (4 bytes)
		uint32_t checksum;
		raw(buffer_read, buffer_read_cursor, buffer_size, checksum, 4);
		checksum =  ntohl(checksum);

		raw(buffer_read, buffer_read_cursor, buffer_size, message, message_size - 4 - 1 - 4);

		// ... string crc32 (4 bytes)
/*		boost::crc_32_type result;
		result.process_bytes(message.c_str(), message.length());
		raw(stream, htonl(result.checksum()));*/

		// ... message string bytes
		//stream >> message;
	}

	template <typename Data>
	static void raw(char* buffer_read, uint32_t &buffer_read_cursor, const uint32_t buffer_size, Data& data, size_t length)
	{
		std::stringstream stream;
		unsigned counter = 0;

		std::cout << "raw:" << length << ":[";
		while(counter < length)
		{
			std::cout <<  buffer_read[buffer_read_cursor + counter] << "|";
			stream << buffer_read[buffer_read_cursor + counter];
			++counter;
		}
		std::cout << "]" << std::endl;

		stream.readsome(reinterpret_cast<char*>(&data), length);
		buffer_read_cursor+=length;

		std::cout <<  "rawfinal[" << data << "]" << std::endl;
	}

	template <typename Data>
	static std::ostream& raw(std::ostream& stream, const Data& data)
	{
		stream.write(reinterpret_cast<const char*>(&data), sizeof(Data));
		return stream;
	}
};

}

#endif /* KAFKA_ENCODER_HELPER_HPP_ */
