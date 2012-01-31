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
 * Created on: 21 Jun 2011
 * Author: Ben Gray (@benjamg)
 */

#ifndef KAFKA_ENCODER_HPP_
#define KAFKA_ENCODER_HPP_

#include <boost/foreach.hpp>
#include "encoder_consumer_helper.hpp"

#include <iostream>

namespace kafkaconnect {


//soruce: http://stackoverflow.com/questions/3022552/is-there-any-standard-htonl-like-function-for-64-bits-integers-in-c
uint64_t htonll(uint64_t value)
{
    // The answer is 42
    static const int num = 42;

    // Check the endianness
    if (*reinterpret_cast<const char*>(&num) == num)
    {
        const uint32_t high_part = htonl(static_cast<uint32_t>(value >> 32));
        const uint32_t low_part = htonl(static_cast<uint32_t>(value & 0xFFFFFFFFLL));

        return (static_cast<uint64_t>(low_part) << 32) | high_part;
    } else
    {
        return value;
    }
}

void encode_consumer_request_size(std::ostream& stream, const std::string& topic)
{

/*
	source: https://github.com/dsully/pykafka/blob/master/kafka/consumer.py
	# REQUEST TYPE ID + TOPIC LENGTH + TOPIC + PARTITION + OFFSET + MAX SIZE
	def request_size(self):
		return 2 + 2 + len(self.topic) + 4 + 8 + 4

	def encode_request_size(self):
		return struct.pack('>i', self.request_size())

*/
	// Packet format is ... packet size (4 bytes)
	encoder_consumer_helper::raw(stream, htonl(2 + 2 + topic.size() + 4 + 8 + 4));
}


void encode_consumer_request(std::ostream& stream, const std::string& topic, const uint32_t partition)
{
/*
	source: https://github.com/dsully/pykafka/blob/master/kafka/consumer.py
    def encode_request(self):
		length = len(self.topic)
	    return struct.pack('>HH%dsiQi' % length, self.request_type, length, self.topic, self.partition, self.offset, self.max_size)*/

	// ... topic string size (2 bytes) & topic string
	//encoder_consumer_helper::raw(stream, htonl(2 + 2 + topic.size() + 4 + 8 + 4));

	// ... kafka format number (2 bytes)
	encoder_consumer_helper::raw(stream, htons(kafka_format_version));

	encoder_consumer_helper::raw(stream, htons(topic.size()));

	stream << topic;

	// ... partition (4 bytes)
	encoder_consumer_helper::raw(stream, htonl(partition));

	// ... offet (8 bytes)
	uint64_t offset = 0;
	encoder_consumer_helper::raw(stream, kafkaconnect::htonll(offset));

	// ... max_size (4 bytes)
	encoder_consumer_helper::raw(stream, htonl(max_size));
}

template <typename List>
void decode_consumer(char* buffer_read, const uint32_t total_bytes_to_process, List& messages)
{
	messages.clear();
	uint32_t processed_bytes_cursor = 0;

	// source: https://cwiki.apache.org/confluence/display/KAFKA/Writing+a+Driver+for+Kafka
	uint16_t error_code;
	std::memcpy(&error_code, buffer_read + processed_bytes_cursor, sizeof(error_code));
	processed_bytes_cursor += sizeof(error_code);

	while (processed_bytes_cursor < total_bytes_to_process)
	{
		/*
		A message. The format of an N byte message is the following:
		 4 message size <-- already parsed in caller.
		 1 byte "magic" identifier to allow format changes
		 4 byte CRC32 of the payload
		 N - 5 byte payload
		*/

		uint32_t message_size;
		std::memcpy(&message_size, buffer_read + processed_bytes_cursor, sizeof(message_size));
		processed_bytes_cursor += sizeof(message_size);
		message_size =  ntohl(message_size);

		// ... magic number (1 byte)
		uint8_t message_format_magic_number;
		std::memcpy(&message_format_magic_number, buffer_read + processed_bytes_cursor, sizeof(message_format_magic_number));
		processed_bytes_cursor += sizeof(message_format_magic_number);
		message_format_magic_number =  ntohl(message_format_magic_number);

		// ... string crc32 (4 bytes)
		uint32_t checksum;
		std::memcpy(&checksum, buffer_read + processed_bytes_cursor, sizeof(checksum));
		processed_bytes_cursor += sizeof(checksum);
		checksum =  ntohl(checksum);

		// ... message string bytes
		uint32_t length = message_size - 1 - 4 + 1; // +1 for null terminal character
		char *msg = new char[length];
		std::memcpy(msg, buffer_read + processed_bytes_cursor, length - 1);
		processed_bytes_cursor += length - 1;
		msg[length - 1] = '\0';

		std::string message(msg);
		delete msg;

		std::cout << "[message:" << message << "][size:" << message_size - 1 - 4 << "]" << std::endl;

		// check checksum
		boost::crc_32_type result;
		result.process_bytes(message.c_str(), message.length());
		if (result.checksum() == checksum)
			messages.push_back(message);
	}
}


}

#endif /* KAFKA_ENCODER_HPP_ */
