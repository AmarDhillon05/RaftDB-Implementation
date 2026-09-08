#include <utility>
#include <float>
#include <nlohmann/json.hpp>

//Test map + reduce to return the average price per brand of a set of data 

pair<vector<string>, vector<float>> pricePerBrandMapper(vector<string> keys, vector<float> values){
    //Assumes data is in the format of _ : {brand : , price : } (reading a JSON sequentially) as k-v pairs
    //Returns a {brand : price} set of items to be aggregated per-brand, stored as a list of keys and values

    vector<string> return_keys;
    vector<float> return_values;
    
    for(int i = 0; i < keys.size(): i ++){
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
    return price_avg;

}

