
##Part 1

ok, Imma start wiht https://pdos.csail.mit.edu/6.824/labs/lab-mr.html - but from scratch in cpp and only really building out mapreduce, just to confirm 







we're gonna be implemeting mapreduce to handle rw files



at a high level, map-reduce involes a coordinator to call map on workers for ops and compile them (the "reduction")



I'm looking at this off of wikipedia to confirm what it means:





Prepare the Map() input – the "MapReduce system" designates Map processors, assigns the input key K1 that each processor would work on, and provides that processor with all the input data associated with that key.



Run the user-provided Map() code – Map() is run exactly once for each K1 key, generating output organized by key K2.



"Shuffle" the Map output to the Reduce processors – the MapReduce system designates Reduce processors, assigns the K2 key each processor should work on, and provides that processor with all the Map-generated data associated with that key.



Run the user-provided Reduce() code – Reduce() is run exactly once for each K2 key produced by the Map step.



Produce the final output – the MapReduce system collects all the Reduce output, and sorts it by K2 to produce the final outcome.



wikipedia implies this is list to list, not list to single value, which challenges what I first thought. let me look at an archietcture diagram to confrim, then delve into the paper to break down what I'm actually implemetning



ok I went a ill bit  further in the wikipedia and found 



function Map is
    input: integer K1 between 1 and 1100, representing a batch of 1 million social.person records
    for each social.person record in the K1 batch do
        let Y be the person's age
        let N be the number of contacts the person has
        produce one output record (Y,(N,1))
    repeat
end function

function Reduce is
    input: age (in years) Y
    for each input record (Y,(N,C)) do
        Accumulate in S the sum of N*C
        Accumulate in Cnew the sum of C
    repeat
    let A be S/Cnew
    produce one output record (Y,(A,Cnew))
end function

which implies we are aggregating partial counts - I'll still look at the diagram. This does confirm something good thoguh - that we're essentially writing a wrapper that can take custom maps and reduces and handle them in a parallelized manner. 

ok so yea from a diagram it seems that reducer outputs a list and we reduce those outputs, but the exact arch is map -> shuffle -> reduce -> result where map and reduce are fit to your needs. an example would be if I wanna get the averages of each grade in a ddatabase that's distributed, the map handles computing averages of each worker, and then reduce distributes the accumulation once, and then there's one accumulation of the actual reductions - I think I get why we're doing that now. I'd be curious about a tree-style reduction, but that would be for later.



ok, now looking at the lab specifics + paper so I know specific impl dtails

we're using RPC - I'm gonna assume there's a cpp impl for that but worst case I don't really think I need to use RPC other than bc its intuitive



quick note - this is a hefty ass week project for an mit class i wonder if they do these at the big maryland id love to check em out 



ok, i really need to get map (k1,v1) → list(k2,v2)

reduce (k2,list(v2)) → list(v2 in my head - bc those are gonna be the things we use for map and reduce templates. I'd assume we have a abc in cpp that will hold this. 



time to read the document. one thing to note is that we'll have to add our own implementations of map and reduce given data handlers. it might be nice to have some kinda functiono that takes a normal sequential naive data call and makes it a mapreduce, but that's kinda hard given aggregation logic is custom. 



it looks like map preiodically writes to local disk and then reduce aggregates - that makes sense with the shuffle step since shuffle would just be in which node takes which data. 



the paper does talk about there being a set of R files each map task creates for one reduce task - that makes me think that map tasks map to a set of reduce tasks but I know that's not true. I do know each reduce task reads from a set of reduce files though - I'm assuming there's a set o fR files that map tasks variously write to? Wait no that's just stupid oyu'd have to maintain more references. The best approach I think is map tasks each writing to one temp file, and then reduce tasks read from a shuffled set of files per reduce task. 



one thing to note is that we have a et of "M" and "R" tasks, but a smaller amount of machines, meaning we're supposed to repurposes machines. I'd assume its something like a worker is basically waiting to be assigned a job, and then the master assigns jobs based on free workers. 



section 3.6 on backup/stragglers implies it distributes work of a long-running task - I'm gonna treat that as an extra item and try to make it so our system can handle that easily. 



ok, final thing to handle is just how keys are navigated and set that in stone, so that we can actually code this up. so, the map function takes in all the keys + queries requested, and splits them into a set o fwhich map workers take which keys. each map worker produces an aggregate, so a "list of keys" into a temp file. the reducer workers then take in the list of keys as well - but it seems that reducers are responsible for reducing certain keys, which would make sense but mean that one reducer has to essentially sift through all the worker intermediate files and find their respective keys. the idea, though, is that the final result is shard-like (at least for the partiton expansion, which makes most sense to me), where if you query the reducer output, it redirects your query to the specific reducer output file - meaning each reducer has to output the full aggregate for their specfic key. let me just make sure this is true 



ok, so in that case, the implementation is going to be: we have an orchestrator that does each of the 5 steps sequentially (so detects when workers are done, assigns reducers, etc.) since we're using RPC and reusing nodes for diff tasks, I'm  thinking of a ws-like protocol where the central service can either communicate workers to execute certain code or poll them (kinda like two separate paths, but I'd need to brush up on RPC). For now, what i wanna start with since this is meant for a Raft KV-store is make two sample map and reduce operations I'lluse for basic testing, as well as generate some distrbuted data. 



ok, so whaat I'm thinking is then 

let's randomly generate some test data of a kv-cache of users - so a simple set of 10 files, JSON-dumped, containing a large amount of both items (im thinking i eventually use this for distributed loading and parsing + filtering of fb  marketplace postings). our map workers are each gonna have assigned files and compute the keys - so essentially, the map funciton will take in a list of files, compute a list of keys from the files it finds, and bump the key values of each file it has to write to. the reducer function will then have to read from a certain set of keys (so take those keys as input), resolve them to files, and write to a final file. the ida is that these are plug-and-play functionality and the map-reduce handler will return the final object to take in and return database queries. I wanna breifly think if I can make the map-reduce handler work outside of kV-stores, since its data so we'd wannna - lets think abt what the map reduce orchestrator needs to do. 

So basically, it takes a map function and a reduce function, as well as an "M" and "R" for the amount of workers. It needs to assign map and reduce workers, so let's say the map reduce orchestrator is given a file/source list and evenly distributes it to the map. the map and reduce are then responsible for communicating with each other, but ket's have wrapper classes of some sort for the workers to handle that. so in what we're gonna do rn (the mapping and reducing function for testing), we need to handl writing that logic of sharing mappers and reducers. the whole operation also needs to output an index to resovle things - let's actually have that be a new argument (the partition function essentially) for the orchestrator.



you're right - i should've reramed that purpose. so essentially, the idea is that map func takes a list of keys and outputs data of the keys it finds (which still sounds like the input is files and the output is the aggregated data). reducer then takes in the keys its responsible for, resolves the files to look for itself (so it assumes that they're created in-between), and then aggregates and returns that data. hat's all - all the file writing, navigation between keys, is done by the orchestrator


yea, so im thinking in the representation that nobody knowsthe keyspace (it's never passed in or hardcoded) and the format is map takes k1 -> k2 and reducer takes k2 -> values for v2. that's why i said that map could take files and output the keys it found - we can resolve keys to hashes if we accumulate a list of keys (since the orchestrator has to write map data to file anyway), and then pass in the hashes (so what file to read) for each reducer. that actually makes more sense, now that I thknk abt it, bc reducers don't know all the data, where as what i just realized is we need all the keys to form the hash correctly

we have a deterministic method to go from keys to which file, right? That's what the orchestrator is doing. So, why not just move the function to do that to the reducer? I'm not saying its necessarily more practical, I'm just asking why it isn't possible

(LOOk at big picture dumb fuck)

ok so some of that is to interpretion, but that's teh flow. my idea was that mappers and reducers can be used for other stuf ftoo, like general aggregates on single files. so on that rep, it makes sense for a reducer to read a single file (like its partition), so have reducers take in an input of files (partitions to read). on the other hand tho, the job of a mapper can be used to compute an aggreaget for one file, so have it take a list of files (data files) as well, but why not just have it output data, andthen havethe map-reduce actually write the data? Is it bc that's slow since writing data can likely be parallelized? What about guarding intermediate files - tho that can p much be done wiht a lock. so  the goal isn't really isolating functionality, its parallelizing the most amountof work. 

ok the onyl other conernis with cnocurrency - multiple keys can be edited at the same tie, so how do we guard that? Esp since we're liekly storing aggregates? since partitions themselves carry multiple keys, and the output for one mapper can land in multiple partitions, it's entirely likely we're doing single-key updates on a partition hit.

we could do a per-key mutex for that, since technically the idea is that every key can only be edited by one mapper at a time

I don't mean with code, I mean idealogically. you said that each mapper has a unique set of intermediate files to write to, and each reducer has a set of intermediate files to look at? But, how does that work with the assumption that each intermediate file is a partition that has the total data of a certain key? Because can't any mapper run into  that key?

ok, but we're still operating so that the reducer outputs data form the keys its responsible for, since one reduce routputs one file. so it needs to have/know the partitions that contain that key. Do we just form that as we do mappings? These are the assumptions I'm making, i don't understand how all of them can coexist:







mappers take in a set of database files (per our abstraction) and output the set o f the keys they found in these files 



each mapper has its own set of intermediate files that it writes to (I'm assuming something like it resolves a hash by key, and so the intermediate file is named by both which mapper and which hash/bucket)



each reducer is given the hashes that have its data (as per what we said earlier, we do have buckets, so we could look at the bucket level - but then that means the orchestrator needs to be able to look at that. to be fair, that is just a single pass through all the names of the interpediate files and seeing which ones belong to each bucket 



reducers themselves are responsible for their list of unqiue keys and return aggregates just for those keys



I guess that works 

ok, so then my abstraction would be that the mapping function only takes in files to read and generates keys, with a separate service (dataservice ig) to write the keys to their respective bucket based on the mapper. then, the reducer expects to read the list of files to look at automatically (we could hae the dataservice also perform those assignments, since it's anyways collecting the data of what buckets exist),

youre right i missed that
missed the convention of predetemrmined file names






(DID SOME THINKING + MEDITATION)

ok dataloader is done, now let's define what the mapper takes and the result takes:

the map function (the part of the functionality that is not reusable) takes in the file paths it has to process, and is responsible to turn that into k, v data. 

the reduce function takes in a list of files and is responsible for aggregating those results. 

that's the definition I'll use - neither of them should be doing any file work yet.

that's something I glossed over - why are we sorting? Lemme go check in the paper rq

yea, so a subset - but the idea is that a mapper writes multiple partitions (and reads multple files) and a reducer reads multiple parittions an writes to a single file. ok, so to that first one at base just reads files sequentially and does aggregate, so only aggregate data is stored in memory. if we don't want aggregate data stored in memory, then perhaps we can do one-by-one categories, but this shouldn't be an issue for RAM. for readers, however, the issue is that the naive approach is look at all flies and collect aggregates, which means that we need all files in memory - except I don't think we do, since can we not just sequentially look at it again? 





the idea of sorting is that, from what I understand, a reducer has multiple file references open and it merges only the memory for the aggregates. That implies storing whole aggregates is the problem. That arises two questions - 1). why not just do it where partition 1 contains all of key 1's aggregates, then all of key's 2 aggregtes, etc.  until full, so you open a new file only when you need to while only needing one data's aggregate in memory? Two, why doesn't that make it so we have to get each aggregate separately in the map - or is that because we'd take too long


ok ima lil confused. the conventions I'm operating under are 

mapper takes in files, outputs partitions, where a mapper has its own partitions, but diff partitions are responsible for one key, per mapper. so let's say buckets numbered 0 should have key A and B. Then file "0_0" contains the keys A and B's data found by mapper 0, "1_0" is the ones found by mapper 1, etc. 

reducers then take in the files correlating to the keys it is responsible for. so let's say one reducer needs all keys that would go in a 0 bucket. it then reads from "1_0", "0_0", etc. 

the reaosn I'm asking this is because if this is the way it's orchestrated, the mapper neeeds full key data/a full key aggregation at some point, which means it needs to read all the data and keep track of alll the aggregates anyway. I get how sotring makesit so the reduceer doesn't store everything in memory, but I don't get why it hs to b ethe merge-style sort you outlined.


ok something caught me there, so I'm gonna look. I thought mapping WAS aggregating results, not just arranging, but let me go seee real quick



ok so yea it seems more like map rearranges data (or relevant data) in a way that's easy to read. so for a word counter, let's say it has multiple documents and we're tryna calculate the existence of a couple of words, for each doc for each word we'll emit which word was found where, and then that word's occurence is logged in a location so that it can be used later. that makes sense why we're not doing any aggregation - essentially, the mapper doesn't wanna hold anything in memory and really just wants to stream whatever's necessary. 



so it returns "occurences" as they come, which is why we can't aggregate one list of all occurences of one word, then another - because that would require holding in memory. but, we're still storign, which is kinda weird. are we just dumping into partitions, and then sorting partitions so partitions cna be read? That doesn't work, and that raises another question - we know that every time we see "dog", it'll be resolved to the same bucket. does that not mean every time we see dog, it'll go oto the same partition. 



Ohh, i get it - partitions are sorted, but have to be merged bc they're all diff data. one partition is for one mapper, and another is for another mapper, so there's no wya they can be read as one sequential wave. 

ok, so a mapper takes a list of files. we're alr paralelizing that that level, so Im gonna assume mappers process files one-by-one per file. so I'm gonna interperet the whole k, v thing as a kinda of batch, where we input a batch to map(), the map() returns the relevant data, and the data is then put into its partition (I guess it's inserted into its partition). then reduce() makes sense to do a merge-wise stream, but that's not reduce's behavior to do - we knowit salways an aggregate, so let's just also have the kv -> kv principle

that's true -and it could allow for more paralleliaiotn later if that makes sense. 

ok just to confirm so I build around ti:



we have an orchestrator and nodes that either operate as mappers or reducers. mappers are responsible for certain files, reducers are responsible for certain partitions. I don't know exactly how I'd make the communication, but it would be something like a reducer can recieve commands to start or stop working, and cna be interrupted - I'm not sure how to do that in a single thred, but that's not hte problem 



Sequentially, the mapper would read files, give the file (either a chunk or a whole file) to a map() function, that will resolve the relevant k, v data that needst to be aggregated, of which the mapper will dump into the correct spot (either via an insertion into the file if that's possible or by dong a final sort afterwards via loading and sorting the whole partition). 



The reducer part will then assign certain reducers to partitions once that's done. each reducer will resolve what files to look at, and run a merge-style read and agggergation where it gives k, v either a batch of stuff it loaded or just the results of the merge and uses the aggregate (or it could just give it all of one key - that makes more sense acc so the aggregatino is all up to the reducer function and so reduce ris more plug-and-play). Once it's odne, it then outputs a single aggregated data file for hat partition, whicih is then used to be read (we'll worry about redirecting requests to partitions form the outside later). 




ok just to confirm so I build around ti:

we have an orchestrator and nodes that either operate as mappers or reducers. mappers are responsible for certain files, reducers are responsible for certain partitions. I don't know exactly how I'd make the communication, but it would be something like a reducer can recieve commands to start or stop working, and cna be interrupted - I'm not sure how to do that in a single thred, but that's not hte problem 

Sequentially, the mapper would read files, give the file (either a chunk or a whole file) to a map() function, that will resolve the relevant k, v data that needst to be aggregated, of which the mapper will dump into the correct spot (either via an insertion into the file if that's possible or by dong a final sort afterwards via loading and sorting the whole partition). 

The reducer part will then assign certain reducers to partitions once that's done. each reducer will resolve what files to look at, and run a merge-style read and agggergation where it gives k, v either a batch of stuff it loaded or just the results of the merge and uses the aggregate (or it could just give it all of one key - that makes more sense acc so the aggregatino is all up to the reducer function and so reduce ris more plug-and-play). Once it's odne, it then outputs a single aggregated data file for hat partition, whicih is then used to be read (we'll worry about redirecting requests to partitions form the outside later).



I wanna make it threaded tho - we have rpc so honestly why not just have proc for each. ok, so the principle is that we have a set of workers, and if one worker is stalling, we choose a diff worker. by definition, rpc would allow us to call a worker without any polling no? I'm envisioning something like a worker periodically polls for if it should continue, and abandons if it can. don't clarify my architecutre but do tell me why that's wrong if it's wrong


Alright. Let's go through the advantages of each and think about why I would do one versus the other. So for each task, both assigning and reading... or so both server to worker or worker to server, we can either do polling where the person who's sending the message sends a message or not polling where the person to receive the message pulls. That made... that was the opposite, by the way. But you know what I mean? So if we're... okay. Let's just go over it. the simplest method would just be having your own set of workers that exist and each time you wanna work at work, you just send a message. The issue with that is that, one, we might have to do threading. So, like, a thread intercepts messages. And, two, even if we weren't threading, we're doing polling, then it would just be more complex, whereas polling could read a state, which would make sense. So we handle basically every atomic call, kind of, where we do a call, we check state, and then based on state, we do... we need to. We could easily work around that. Now that's good reason for just doing polling, you know, over code complicity, but I wanna find out if I can justify another reason. So you said that MIT picks worker initiated polling because we don't wanna know every worker's address upfront. Right? Okay. And so the idea is that if we're actively assigning work, we need to maintain all the workers available. Whereas if we are not assigning work and we're being asked for work, then we don't need to search for availability because availability will come to us. So we only maintain a list of the nodes that are available. Now we could obviously have it where nodes just communicate when they're finished. But that... actually, wait. Why can't we just do that? So let's say we start with all nodes is available, and we gradually make notes unavailable. But... no. because then we need the whole waiting procedure is another thing. Right? Okay. So, actually, that's another thing. How would we do the waiting procedure? Ideally, what we would have is that workers are paralyzed. So would we just... actually, yeah, we look he could do that where we basically start a thread for each mapper and each reducer, honestly, where we start a thread, and each thread is responsible for making sure it's work to gets done. So it sends messages when it needs to... it receives data when it needs to. Only thing is if we are using RPC, we can't receive data threaded. So that actually might not make that much sense. So I guess we're not gonna do it threaded, you know, handling. But what we could do is... how about we have something where every second we tick... or not every second we tick. Every second we check which items have expired. And if they've expired, we reassign them. So let's say the mapper basically maintains all of its active tasks, and it maintains it as task in deadline. If it receives a task, Or I guess it'll be task deadline and what the task is responsible for for, like, which mapper ID let's say. So, basically, for each tick, it'll check if any mappers are taking too long. And if a mapper is taking too long, then it can change the mapper's state. Either it can... okay. Okay. Let's fuck it. I'm I'm going way too far. Alright. Let's go over... let's say the basics. Right? Actually, no. No. because this is part of it. Okay. Let's just go top down because if we go into communication, then we're not really gonna get the diagnosis of how we integrate that communication clearly. So the really loop is gonna be... we need to set out a fleet of mappers, have them all run their job, detect when they're all done, and once they're all done, communicate to a set of producers. The way we could... we also need the reassignment of mappers. So, yeah, one way is each tick we see if any mapper is taking too long and if a mapper is taking too long, we reassign So, like, some kinda heap just to manage when the mapper is taking too long. That would honestly... I mean, you could literally just do a sorted list if you really wanted to. So, like, yeah, basically, just checking. Is this mapper expired if so? So they're to a new worker. Now for that, we also need to track which maps are full filled. So just keep a basic hash map of, like, yeah, which map is full filled. Right? Now you also need to track available maps because you need to know which maps that you're able to send to. Alright. So, basically, you'll be tracking both in the heap, the maps that are working, and the maps that you can assign. So the workflow you would be... you first assign every single map, map ID. Let's say a map ID is a specific mapper. And then recurrently, you... first, you handle requests. Right? So if a mapper is done, it'll send you a request. You specify that doesn't need to be completed anymore. Then for the heap, you repeatedly take mappers that are taking too long. you check if they're taking too long. And if they're taking too long, you reassign them. But if they're taking too long and they're done, you obviously don't reassign them. Now the final thing to tackle is if we have this set up, how do you tell a mapper to be done? And... okay. And how do you manage, you know, being free? We could have something like we're each worker. For each worker, you maintain a state, but I'm trying to think. Can you just send a call? Ideally, it would be... you can just send a call to the worker, and the worker will resolve that call and realize it's done. But then you would need multithreading. Like, you'd be able... you need to be able to both do the worker work as well as handle. Yeah. Oh, like, you need to be able to... okay. You both need to be handling the worker's work as well as the whole multi... you know, so you'd have to use a multithreaded thing for that The other option is straight polling where all workers do is just poll and send messages. Send messages is good for, like, you know, being available. So, essentially, one tick of... actually, wait. Let me hold on a second. Okay. That's another thing. Would it be possible where... let's say we're communicating to a node that I need to stop doing work Could it be that we on our PC just send... stop doing work to this one node? And then that node's loop would be... first, it pulls. So it knows to stop doing work or not, and then it does the work if it needs to. Is that a possible thing we can do? So let's say every node has its own loop based on its internal state and its assigned task. So if it has work to do, then it'll do work. Otherwise, it won't, kind of. Alright. That was long. But


ok so just let the compute run but only save the result if its the one we wanna handle. out of curiosity, 1). why is it fine to let stragglers just run, and 2). why can we only have the server be parallel? what if we didn't want any to be parallel - would that just mean we maintain more state? Is that such a bad tradeoff that we're not doing it?