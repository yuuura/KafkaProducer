#pragma once

#include <string>
#include "json_writer.h"
#include <ctime>

class JSONMessage
{
	
public:
	// to be JSON'ised
	int id;
	//std::time_t timestamp;
	std::string timestamp;
	// each class requires a public serialize function
	void serialize(JSON::Adapter& adapter);
};