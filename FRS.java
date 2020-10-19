import java.util.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/*** Apache Hadoop Import Files  ***/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class FRS {

    //construct a new class that represents user and their corresponding friends
    //https://www.cnblogs.com/wuyudong/p/hadoop-writable.html
    //http://yoyzhou.github.io/blog/2013/05/09/hadoop-serialization-and-writable-object-1/

    static public class FrsWritable implements Writable {

        public Long userName, mutFriends;
        //parametic constructor
        public FrsWritable(Long userName, Long mutFriends) {
            this.userName = userName;
            this.mutFriends = mutFriends;
        }
        //overwrite the functions from writable
        @Override
        public void readFields(DataInput in) throws IOException {
            userName = in.readLong();
            mutFriends = in.readLong();
        }
    
        public void write(DataOutput out) throws IOException {
            out.writeLong(userName);
            out.writeLong(mutFriends);
        }

        public String toString() {
            return " CurrentUser: " + Long.toString(userName) + " MutualFriends: " + Long.toString(mutFriends);
        }


        public FrsWritable() {
            this(-1L, -1L);
        }

    }

    public static class FriendRecommandSystemMap extends Mapper<LongWritable, Text, LongWritable, FrsWritable> {
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //seperate and record input of user and friend based on TAB <User><TAB><Friends>
            String input[] = value.toString().split("\t");
            //the user before tab
            Long userBegin = Long.parseLong(input[0]); 	
            //the list of existing friends	
            List<Long> userEnd = new ArrayList<Long>(); 	
            //if we have both user and existing friends in the input
            if (input.length == 2) {
                //record the list of friends
                StringTokenizer token = new StringTokenizer(input[1], ",");
                //parse through the list of friends
                while (token.hasMoreTokens()) {
                    Long CurrentUser = Long.parseLong(token.nextToken());
                    userEnd.add(CurrentUser);
                    //match the <User> with each of the <Friends>
                    context.write(new LongWritable(userBegin), new FrsWritable(CurrentUser, -1L));
                }
                //match <Friends> with each of the <Friends>
                for (int i = 0; i < userEnd.size(); i++) {
                    for (int j = i + 1; j < userEnd.size(); j++) {
                        context.write(new LongWritable(userEnd.get(i)), new FrsWritable((userEnd.get(j)), userBegin));
                        context.write(new LongWritable(userEnd.get(j)), new FrsWritable((userEnd.get(i)), userBegin));
                    }
                }
            }
        }
    }


    public static class FriendRecommandSystemReduce extends Reducer<LongWritable, FrsWritable, LongWritable, Text> {
        //override
        public void reduce(LongWritable key, Iterable<FrsWritable> values, Context context) throws IOException, InterruptedException {
            
            final java.util.Map<Long, List<Long>> mutFriends = new HashMap<Long, List<Long>>();
            for (FrsWritable value : values) {

                final Boolean existingFriend = (value.mutFriends == -1);
                final Long CurrentUser = value.userName;
                final Long mutualFriend = value.mutFriends;

                if (mutFriends.containsKey(CurrentUser)) {
                    //if these two are friends already
                    if (existingFriend == true) {
                        mutFriends.put(CurrentUser, null);
                    } 
                    //if these two people are not existing friends
                    else if (mutFriends.get(CurrentUser) != null) {
                        mutFriends.get(CurrentUser).add(mutualFriend);
                    }
                } 
                
                else {
                    //if these two people are not existing friends
                    if (existingFriend == false) {
                        mutFriends.put(CurrentUser, new ArrayList<Long>() {
                            {
                                add(mutualFriend);
                            }
                        });
                    } 
                    //if these two are friends already
                    else {
                        mutFriends.put(CurrentUser, null);
                    }
                }
            }

            // Sorting all the Mutual friends using Tree Map
            java.util.SortedMap<Long, List<Long>> sortFriends = new TreeMap<Long, List<Long>>(new Comparator<Long>() {
                public int compare(Long key1, Long key2) {
                    Integer idOne = mutFriends.get(key1).size();
                    Integer idTwo = mutFriends.get(key2).size();
                    //ordered in decreasing number of mutual friends
                    if (idOne > idTwo) {
                        return -1;
                    } 
                    //users with the same number of mutual friends, then output those user IDs in numerically ascending order.
                    else if (idOne.equals(idTwo) && key1 < key2) {
                        return -1;
                    } 
                    else {
                        return 1;
                    }
                }
            });

            for (java.util.Map.Entry<Long, List<Long>> entry : mutFriends.entrySet()) {
                if (entry.getValue() != null) {
                    sortFriends.put(entry.getKey(), entry.getValue());
                }
            }

            Integer i = 0;

            String recommendList = "";
            //iterate through the list of recommended friend
            for (java.util.Map.Entry<Long, List<Long>> entry : sortFriends.entrySet()) {
                //the first recommended friend in the list
                if (i == 0) {
                    recommendList = entry.getKey().toString();
                } 
                //subsequent friends
                else if (i < 10){
                    recommendList += "," + entry.getKey().toString();
                }
                ++i;
            }
            context.write(key, new Text(recommendList));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "FriendRecommandSystem");
        job.setJarByClass(FRS.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FrsWritable.class);
        job.setMapperClass(FriendRecommandSystemMap.class);
        job.setReducerClass(FriendRecommandSystemReduce.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem outFs = new Path(args[1]).getFileSystem(conf);
        outFs.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
