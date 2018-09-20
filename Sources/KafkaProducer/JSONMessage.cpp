#include "JSONMessage.h"


void JSONMessage::serialize(JSON::Adapter& adapter)
{
	// this pattern is required 
	JSON::Class root(adapter, "JSONMessage");
	JSON_E(adapter, id);
	// this is the last member variable we serialize so use the _T variant
	JSON_T(adapter, timestamp);
}


