#!/usr/bin/env bash
# Map reduce tests - runs each app sequentially to get the right answer, then runs it distributed
# (1 coordinator + a few workers) and checks the outputs match. Run from the repo root: make test-mr

ROOT=$(pwd)
BIN=$ROOT/build
failed=0

rm -rf mr-tmp && mkdir mr-tmp && cd mr-tmp || exit 1

# Inputs: generated text for wc/crash, the dbnode shoe data for shoes
mkdir inputs
$BIN/create_text inputs
TEXT_INPUTS=$(ls inputs/text_*.txt)
SHOE_INPUTS=$(ls $ROOT/map_reduce/test/data/dbnode_*)


# Helper: run_job app.so n_workers inputs... -> leaves sorted output in mr-all
run_job() {
    app=$1; workers=$2; shift 2
    rm -f mr-out* mr-*-*

    $BIN/mrcoordinator "$@" &
    coord=$!
    sleep 1

    for i in $(seq 1 $workers); do
        $BIN/mrworker $BIN/$app &
    done

    wait $coord
    sort mr-out* | grep . > mr-all
}


# Helper: compare mr-all to the sequential answer for the same app + inputs
check() {
    name=$1; app=$2; shift 2
    $BIN/mrsequential $BIN/$app "$@"
    sort mr-out-0 > mr-correct
    rm -f mr-out-0

    if cmp -s mr-all mr-correct; then
        echo "--- $name test: PASS"
    else
        echo "--- $name output does not match sequential, FAIL"
        failed=1
    fi
}


echo "*** Starting wc test."
run_job wc.so 3 $TEXT_INPUTS
check wc wc.so $TEXT_INPUTS
wait

echo "*** Starting shoes (price per brand) test."
run_job shoes.so 3 $SHOE_INPUTS
check shoes shoes.so $SHOE_INPUTS
cat mr-all
wait

echo "*** Starting early exit test."
# No worker should exit (and no output should be finalized) before the whole job is done -
# check that mr-out files only show up once all reduces are reported
rm -f mr-out* mr-*-*
$BIN/mrcoordinator $TEXT_INPUTS &
coord=$!
sleep 1
pids="$coord"
for i in 1 2 3; do $BIN/mrworker $BIN/wc.so & pids="$pids $!"; done
# poll until the first of them exits - the first process to exit is either the coordinator or a
# worker told to exit, and either way all 10 reduce outputs must already exist
while true; do
    alive=0
    for p in $pids; do kill -0 $p 2>/dev/null && alive=$((alive + 1)); done
    [ $alive -lt 4 ] && break
    sleep 0.1
done
outs=$(ls mr-out-* 2>/dev/null | wc -l)
wait
if [ "$outs" -eq 10 ]; then
    echo "--- early exit test: PASS"
else
    echo "--- something exited before the job finished ($outs/10 outputs), FAIL"
    failed=1
fi

echo "*** Starting crash test."
# crash.so randomly kills/stalls workers, so keep restarting workers until the coordinator is done
rm -f mr-out* mr-*-*
$BIN/mrcoordinator $TEXT_INPUTS &
coord=$!
sleep 1
( while kill -0 $coord 2>/dev/null; do $BIN/mrworker $BIN/crash.so; sleep 0.2; done ) &
( while kill -0 $coord 2>/dev/null; do $BIN/mrworker $BIN/crash.so; sleep 0.2; done ) &
( while kill -0 $coord 2>/dev/null; do $BIN/mrworker $BIN/crash.so; sleep 0.2; done ) &
wait $coord
sort mr-out* | grep . > mr-all
check crash wc.so $TEXT_INPUTS
wait

if [ $failed -eq 0 ]; then
    echo "*** PASSED ALL TESTS"
else
    echo "*** FAILED SOME TESTS"
    exit 1
fi
