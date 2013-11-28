package toutiao;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf; 
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.lib.CombineFileRecordReader;
import org.apache.hadoop.mapred.lib.CombineFileSplit;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.net.NetworkTopology;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.lang.ArrayUtils;

import toutiao.NFileRecordReader;


public class NFileInputFormat extends FileInputFormat<Text, Text> {
    static final String NUM_INPUT_FILES = "mapreduce.input.num.files";
    public static final Log LOG =
          LogFactory.getLog(NFileInputFormat.class);

    public RecordReader<Text, Text> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException{
        return new CombineFileRecordReader<Text, Text>(job, (CombineFileSplit)split, reporter, (Class)NFileRecordReader.class);
    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file){
        return false;
    }

    @Override
    public InputSplit [] getSplits(JobConf conf, int numSplits) throws IOException{
        FileStatus[] files = listStatus(conf);
        // Save the number of input files in the job-conf
        conf.setLong(NUM_INPUT_FILES, files.length);
        for (FileStatus file: files) {                // check we have valid files
            if (file.isDir()) {
                throw new IOException("Not a file: "+ file.getPath());
            }
        }                            
        numSplits = (numSplits == 0 ? 1 : numSplits); 
        List<CombineFileSplit> splits = new ArrayList<CombineFileSplit>(numSplits);
        NetworkTopology clusterMap = new NetworkTopology();

        for(int i = 0; i< numSplits; i++){
                CombineFileSplit split = addNewSplit(conf, files, i, numSplits, clusterMap);
                if( split != null){
                    splits.add(split);
                  }  
        }


        return splits.toArray(new CombineFileSplit[splits.size()]);

    }

    private CombineFileSplit addNewSplit(JobConf conf, FileStatus[] files, int splitId, int splitNum, NetworkTopology clusterMap) throws IOException{
        //Path[] fl = new Path[num];
        //long[] offsets = new long[num];
        //long[] lengths = new long[num];
        List<Path> fl = new ArrayList<Path>();
        List<Long> offsets = new ArrayList<Long>();
        List<Long> lengths = new ArrayList<Long>();
        long totalLength = 0;
        List<BlockLocation> blkLocations = new ArrayList<BlockLocation>();
        Map<String, Integer> hostsMap = new HashMap<String,Integer>();
        for(int j=0; j<files.length;j++){
            if( j % splitNum != splitId)
                continue;
            FileStatus file = files[j];
            Path path = file.getPath();
            long length = file.getLen();
            if(length == 0 )
            {
                continue;
            }
            FileSystem fs = path.getFileSystem(conf);
            BlockLocation[] locations = fs.getFileBlockLocations(file, 0, length);
            //LOG.info("file :"+ path +", location length is:  " + locations.length);

            String[] hosts = getSplitHosts(locations, 0, length, clusterMap);
            for(String host: hosts){
                if( hostsMap.containsKey(host) ){
                    Integer counter = hostsMap.get(host);
                    hostsMap.put(host, counter+1);
                }else{
                    hostsMap.put(host, 1 );
                }
            }

            fl.add(path);
            offsets.add(new Long(0));
            lengths.add(new Long(length));
            totalLength += length;
        }
        //hosts.addAll(Arrays.asList(getSplitHosts(blkLocations.toArray(new BlockLocation[blkLocations.size()]), 0 , totalLength, clusterMap)));
        int first = 0;
        int second = 0;
        int third = 0;
        String[] topHosts= new String[3];
        Iterator iter = hostsMap.entrySet().iterator();
        while(iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            int counter = ((Integer) entry.getValue()).intValue();
            if(counter > first){
                topHosts[2] = topHosts[1];
                topHosts[1] = topHosts[0];
                topHosts[0] = (String)entry.getKey();
                third = second;
                second = first;
                first = counter;
            }
            else if(counter > second){

                topHosts[2] = topHosts[1];
                topHosts[1] = (String)entry.getKey();
                third = second;
                second = counter;
            }
            else if(counter > third){
                third = counter;
                topHosts[2] = (String)entry.getKey();
            }
        }
        //Path
        if(fl.size()>0){
            //LOG.info("top hosts is : " + topHosts[0] + ","+topHosts[1]+","+topHosts[2]);
            CombineFileSplit split = new CombineFileSplit(conf, fl.toArray(new Path[fl.size()]), ArrayUtils.toPrimitive(offsets.toArray(new Long[offsets.size()])), ArrayUtils.toPrimitive(lengths.toArray(new Long[lengths.size()])), topHosts);
            return split;
        }else{
            return null;
        }

    }
}

