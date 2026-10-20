#include <utility>
#include <vector>
#include <string>
#include <sstream>
#include <iomanip>
#include <nlohmann/json.hpp>
#include "../mr/common.hpp"

using namespace std;
using json = nlohmann::json;

//Test map + reduce to return the average price per brand of a set of data 
//(data comes from test/create_data.cpp, one dbnode file per map task)


pair<vector<string>, vector<float>> pricePerBrandMapper(vector<string> keys, vector<float> values){
    //Assumes data is in the format of _ : {brand : , price : } (reading a JSON sequentially) as k-v pairs
    //Returns a {brand : price} set of items to be aggregated per-brand, stored as a list of keys and values

    vector<string> return_keys;
    vector<float> return_values;
    
    for(int i = 0; i < keys.size(); i ++){
        string key = keys[i];
        float value = values[i];

        return_keys.push_back(key);
        return_values.push_back(value);
    }

    return { return_keys, return_values };

}


vector<float> priceAvgPerBrandReducer(vector<string> keys, vector<float> values){
    //Assumes data is the keys (all one key value since this is an aggregate) are brands and values are prices
    //Returns the computed average as a single float to be stored into data for this key (brand)

    float price_sum = 0;
    for(float price : values){
        price_sum += price; 
    }

    float price_avg = price_sum / values.size();
    vector<float> return_values = { price_avg };
    return return_values;

}


//Plugin entry points - these adapt the file/string interface the workers use to the mapper + reducer above

extern "C" vector<KeyValue> Map(string filename, string contents){
    //A dbnode file isn't valid JSON as a whole (keys are bare numbers), so read it line by line,
    //where each entry line looks like ` 12: {"brand":"nike","price":100.0}`
    vector<string> brands;
    vector<float> prices;

    stringstream ss(contents);
    string line;
    while(getline(ss, line)){
        size_t colon = line.find(':');
        if(colon == string::npos){
            continue;   //the opening/closing braces
        }

        json entry = json::parse(line.substr(colon + 1));
        brands.push_back(entry["brand"]);
        prices.push_back(entry["price"]);
    }

    auto [keys, values] = pricePerBrandMapper(brands, prices);

    vector<KeyValue> kvs;
    for(int i = 0; i < keys.size(); i ++){
        kvs.push_back({ keys[i], to_string(values[i]) });
    }
    return kvs;
}


extern "C" string Reduce(string key, vector<string> values){
    vector<string> keys(values.size(), key);
    vector<float> prices;
    for(string &v : values){
        prices.push_back(stof(v));
    }

    vector<float> avg = priceAvgPerBrandReducer(keys, prices);

    stringstream out;
    out << fixed << setprecision(2) << avg[0];
    return out.str();
}
