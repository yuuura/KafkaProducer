#pragma comment(lib, "librdkafkacpp.lib")

#include <iostream>
#include <csignal>
#include <ctime>
#include <fstream>
#include <thread>
#include <sstream>
#include <map>

#include "librdkafka\rdkafkacpp.h"
#include "JSONMessage.h"

const std::string property_file = "HttpProperties.txt";
typedef std::map<std::string, std::string> property_map;
property_map configValues;


static bool run = true;
static bool err_conn = false;

static void sigterm(int sig) {
	run = false;
}

class ExampleEventCb : public RdKafka::EventCb {
public:
	void event_cb(RdKafka::Event &event) {
		switch (event.type())
		{
		case RdKafka::Event::EVENT_ERROR:
			std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
				err_conn = true;
			break;

		case RdKafka::Event::EVENT_STATS:
			std::cerr << "\"STATS\": " << event.str() << std::endl;
			break;

		case RdKafka::Event::EVENT_LOG:
			fprintf(stderr, "LOG-%i-%s: %s\n",
				event.severity(), event.fac().c_str(), event.str().c_str());
			break;

		default:
			std::cerr << "EVENT " << event.type() <<
				" (" << RdKafka::err2str(event.err()) << "): " <<
				event.str() << std::endl;
			break;
		}
	}
};

void read_property_file()
{
	std::fstream file;
	file.open(property_file);
	if (!file) {
		std::cerr << "Unable to open file " << property_file << std::endl;
	}
	else {
		std::string broker = "";
		std::string line;
		while (std::getline(file, line))
		{
			std::istringstream is_line(line);
			std::string key;
			if (std::getline(is_line, key, '='))
			{
				std::string value;
				if (key[0] == '#')
					continue;

				if (std::getline(is_line, value))
				{
					configValues[key] = value;
				}
			}
		}
	}
}

int main(int argc, char **argv) {
	
	std::string brokers;
	std::string errstr;
	std::string topic_str;
	int32_t partition = RdKafka::Topic::PARTITION_UA;
	int64_t start_offset = RdKafka::Topic::OFFSET_BEGINNING;

	/*
	* Create configuration objects
	*/
	RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
	RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);

	read_property_file();
	brokers = configValues.find("ip")->second;
	brokers += ":" + configValues.find("port")->second;
	topic_str = configValues.find("topic")->second;
	
	/*
	* Set configuration properties
	*/

	conf->set("metadata.broker.list", brokers, errstr);

	ExampleEventCb ex_event_cb;
	conf->set("event_cb", &ex_event_cb, errstr);

	signal(SIGINT, sigterm);
	signal(SIGTERM, sigterm);

		/*
		* Create producer using accumulated global configuration.
		*/
		RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
		if (!producer) {
			std::cerr << "Failed to create producer: " << errstr << std::endl;
			exit(1);
		}

		std::cout << "% Created producer " << producer->name() << std::endl;

		/*
		* Create topic handle.
		*/
		RdKafka::Topic *topic = RdKafka::Topic::create(producer, topic_str,
			tconf, errstr);
		if (!topic) {
			std::cerr << "Failed to create topic: " << errstr << std::endl;
			exit(1);
		}

		/*
		* Produce to broker.
		*/
		JSONMessage source;
		while (run) {

			auto start = std::chrono::high_resolution_clock::now();
			for (int i = 0; i < 50; i++) {
				auto start2 = std::chrono::high_resolution_clock::now();

				std::time_t result = std::time(nullptr);

				source.id = i;
				source.timestamp = std::string(std::asctime(std::localtime(&result)));
				std::string json = JSON::producer<JSONMessage>::convert(source);

				RdKafka::ErrorCode resp = producer->produce(topic, partition,
						RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
						const_cast<char *>(json.c_str()), json.size(), NULL, NULL);
				if (resp != RdKafka::ERR_NO_ERROR)
					std::cerr << "% Produce failed: " <<
					RdKafka::err2str(resp) << std::endl;
				else
					std::cerr << "% Produced message (id = " << source.id << ", " << json.size() << " bytes)" <<
					std::endl;

				while (err_conn && producer->outq_len() > 0) {
					std::cerr << "Attempting..." << std::endl;
					producer->poll(1000);
				}
				err_conn = false;
				
				producer->poll(0);
				
				std::this_thread::sleep_for(std::chrono::milliseconds(18));
			}
			auto end = std::chrono::high_resolution_clock::now();
			std::chrono::duration<double, std::milli> elapsed = end - start;
			std::cout << "Waited " << elapsed.count() << " ms\n";
		}

		run = true;

		while (run && producer->outq_len() > 0) {
			std::cerr << "Waiting for " << producer->outq_len() << std::endl;
			producer->poll(1000);
		}

		delete topic;
		delete producer;

	/*
	* Wait for RdKafka to decommission.
	* This is not strictly needed (when check outq_len() above), but
	* allows RdKafka to clean up all its resources before the application
	* exits so that memory profilers such as valgrind wont complain about
	* memory leaks.
	*/
	RdKafka::wait_destroyed(5000);

	return 0;
}