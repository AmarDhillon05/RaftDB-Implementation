#include <fstream>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <cstdio>
#include <nlohmann/json.hpp>

using namespace std;
using json = nlohmann::json;


//Helper to create a file + return file stream, or delete and recreate it if it exists 
ofstream recreateFile(string path){
    ofstream ofs(path);
    if(ofs.is_open()){
        int _ = ::remove(path.c_str());
    }
    
    ofstream recreated_ofs(path);
    return recreated_ofs;
}   

int main(){ 

    //Brand choices and ranges for averages
    vector<string> brand_choices = {"nike", "jordan", "adidas", "new balance", "vans"};
    int price_min = 50;
    int price_max = 250;

    //Fills 10 sample files to represent distributed data with shoe data for average filtering

    for(int i = 0; i < 10; i ++){

        //Create new node
        stringstream ss;
        ss << "map_reduce/test/data/dbnode_" << i;
        string path = ss.str();

        //Recreate file and initialize JSON formatting
        ofstream DBNode = recreateFile(path);
        DBNode << "{\n";

        //Randomly assign data entries
        for(int j = 0; j < 100; j ++){
            int rand_idx = rand() % brand_choices.size();
            string brand = brand_choices[rand_idx];
            float price = price_min + (rand() % (price_max - price_min));

            //Create and write formatted JSON entry
            json data_entry;
            data_entry["brand"] = brand;
            data_entry["price"] = price;

            string entry_as_str = data_entry.dump();
            DBNode << " " << j << ": " << entry_as_str << "\n";

        }

        //Close outer JSON formatting and file 
        DBNode << "}";
        DBNode.close();

    }

    return 0;
}



