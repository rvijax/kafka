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

}
