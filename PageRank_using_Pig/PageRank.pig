-- Load Pig UDF for loading data
REGISTER PigPageRank.jar;

-- Pig configration
SET default_parallel $parallelism;
SET pig.noSplitCombination true;

-- Load adjacent matrix into original_data outer data bag with specified data structure (source:chararray,pagerank:double,out:bag{})
original_data = LOAD '$inputFile' USING CustomLoader('$noOfURLs','$iteration') as (source:chararray,pagerank:double,out:bag{});

-- prepage a outgoing links pagerank in records by getting the last column of original_data as (source, self pagerank/SIZE(out))
previous_pagerank = FOREACH original_data GENERATE FLATTEN(out) as source,pagerank/SIZE(out) as pagerank;

-- group all same url into a (source, {original_data#m, previous_pagerank#n}) 
cogroup_graph = COGROUP original_data by source , previous_pagerank by source OUTER;

-- TODO here, foreach cogroup_graph generates source, calculated new pagerank, out in comma list
-- calculate new pagerank, generally it should be (1-$dampingFactor) + $dampingFactor * SUM(previous_pagerank.pagerank) as pagerank
-- you have to check if SUM(previous_pagerank.pagerank) is NULL, if true set it to 0, otherwise just set as is SUM(previous_pagerank.pagerank)
new_pagerank = FOREACH cogroup_graph GENERATE group as source, (1-$dampingFactor) / $noOfURLs + $dampingFactor * (SUM(previous_pagerank.pagerank) is null ? 0 : SUM(previous_pagerank.pagerank)) as pagerank, FLATTEN(original_data.out) as out;

-- Store calculated pagerank into output directory
STORE new_pagerank INTO '$outputFile' ;


