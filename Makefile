# Build everything from the repo root, e.g. `make mr` then `make test-mr`
CXX = g++
CXXFLAGS = -std=c++17 -O2 -g -Wall -Wno-sign-compare -Ilib/json/single_include
LDFLAGS = -pthread -ldl
BUILD = build

# ---------- Lab 1: map reduce ----------

MR_HEADERS = common/rpc.hpp $(wildcard map_reduce/mr/*.hpp)
MR_APPS = $(BUILD)/wc.so $(BUILD)/shoes.so $(BUILD)/crash.so
MR_BINS = $(BUILD)/mrcoordinator $(BUILD)/mrworker $(BUILD)/mrsequential $(BUILD)/create_text

mr: $(MR_BINS) $(MR_APPS)

$(BUILD)/mr%: map_reduce/main/mr%.cpp $(MR_HEADERS)
	@mkdir -p $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@ $(LDFLAGS)

$(BUILD)/%.so: map_reduce/apps/%.cpp map_reduce/mr/common.hpp
	@mkdir -p $(BUILD)
	$(CXX) $(CXXFLAGS) -fPIC -shared $< -o $@

$(BUILD)/create_text: map_reduce/test/create_text.cpp
	@mkdir -p $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@

$(BUILD)/create_data: map_reduce/test/create_data.cpp
	@mkdir -p $(BUILD)
	$(CXX) $(CXXFLAGS) $< -o $@

test-mr: mr
	bash map_reduce/test/test-mr.sh

clean:
	rm -rf $(BUILD) mr-tmp

.PHONY: mr test-mr clean
