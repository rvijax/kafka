#ifndef KAFKA_CONSUMER_HPP_
#define KAFKA_CONSUMER_HPP_

#include <string>
#include <vector>

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/function.hpp>
#include <boost/iostreams/device/array.hpp>
#include <boost/iostreams/stream.hpp>

#include <stdint.h>
#include <fstream>
#include <sstream>

#include "encoder_consumer.hpp"

namespace kafkaconnect {

const uint32_t use_random_partition = 0xFFFFFFFF;

class consumer
{
public:
	typedef boost::function<void(const boost::system::error_code&)> error_handler_function;

	consumer(boost::asio::io_service& io_service, const error_handler_function& error_handler = error_handler_function());
	~consumer();

	void connect(const std::string& hostname, const uint16_t port);
	void connect(const std::string& hostname, const std::string& servicename);

	void close();
	bool is_connected() const;

	// TODO: replace this with a sending of the buffered data so encode is called prior to send this will allow for decoupling from the encoder
	template <typename List>
	bool consume(List& messages, const std::string& topic, const uint32_t partition)
	{
		if (!is_connected())
		{
			return false;
		}

		std::cout << "connected" << std::endl;

		// TODO: make this more efficient with memory allocations.
		boost::asio::streambuf* buffer_write_consumer_request_size = new boost::asio::streambuf();
		std::ostream stream_write_consumer_request_size(buffer_write_consumer_request_size);

		// send consume request size
		kafkaconnect::encode_consumer_request_size(stream_write_consumer_request_size, topic);

		boost::asio::write(	_socket, *buffer_write_consumer_request_size,
			boost::asio::transfer_all());

		// send consume request
		boost::asio::streambuf* buffer_write_consumer_request = new boost::asio::streambuf();
		std::ostream stream_write_consumer_request(buffer_write_consumer_request);

		kafkaconnect::encode_consumer_request(stream_write_consumer_request, topic, partition);

		boost::asio::write( _socket, *buffer_write_consumer_request,
			boost::asio::transfer_all());

		delete buffer_write_consumer_request_size;
		delete buffer_write_consumer_request;
		std::cout << "request send." << std::endl;

		// start read:

		// Read "N" - size of response
		uint32_t header;
		boost::asio::read(
			_socket,
			boost::asio::buffer( &header, sizeof(header) )
		);
		header = htonl(header);
		std::cout << "body is " << header << " bytes" << std::endl;

		// Read the response
		size_t bytes_to_read = header;
		char *buffer_read = new char[bytes_to_read];
		size_t body_read = boost::asio::read(_socket, boost::asio::buffer( buffer_read, bytes_to_read) );
		std::cout << "body read: " << body_read << " bytes" << std::endl;

		//std::istringstream stream_read(buffer_read);
		kafkaconnect::decode_consumer(buffer_read, body_read, messages);

		std::ofstream myfile;
    	myfile.open ("response.txt");

		for (unsigned i=0; i< body_read; i++)
		{
			std::cout << "[" << buffer_read[i] << "]" << std::endl;
			myfile << buffer_read[i];
		}

		delete 	buffer_read;
		return true;
	}


private:
	bool _connected;
	boost::asio::ip::tcp::resolver _resolver;
	boost::asio::ip::tcp::socket _socket;
	error_handler_function _error_handler;

//	boost::array<char, 1> buf;

	void handle_resolve(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);
	void handle_connect(const boost::system::error_code& error_code, boost::asio::ip::tcp::resolver::iterator endpoints);

/*
	void handle_write_request_size(const boost::system::error_code& error_code, boost::asio::streambuf* buffer, const std::string& topic, const uint32_t partition);
	void handle_write_request_body(const boost::system::error_code& error_code, boost::asio::streambuf* buffer);


	//template <typename List>
	void handle_read_request(const boost::system::error_code& error_code, boost::asio::streambuf* buffer);//, List& messages);
	//void handle_read_request(const boost::system::error_code& error_code);
*/

	/* Fail Fast Error Handler Braindump
	 *
	 * If an error handler is not provided in the constructor then the default response is to throw
	 * back the boost error_code from asio as a boost system_error exception.
	 *
	 * Most likely this will cause whatever thread you have processing boost io to terminate unless caught.
	 * This is great on debug systems or anything where you use io polling to process any outstanding io,
	 * however if your io thread is seperate and not monitored it is recommended to pass a handler to
	 * the constructor.
	 */
	inline void fail_fast_error_handler(const boost::system::error_code& error_code)
	{
		if(_error_handler.empty()) { throw boost::system::system_error(error_code); }
		else { _error_handler(error_code); }
	}
};

}

#endif /* KAFKA_CONSUMER_HPP_ */
