package Word_count;

import java.io.IOException;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Test0 {


public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
    public void map(Object key, Text value, Context context)  throws IOException, InterruptedException 
    {
    			Scanner scan = new Scanner(value.toString());
    			while (scan.hasNextLine()) {
    				String temp= scan.nextLine();
    				String[]strArray = temp.split(";");
    		        for(int i = 0; i<strArray.length; i++) {
    		        		String work  = strArray[i];
    		        		int origin = work.length()-1; 
    		        		while(origin>0) {
    		        			if(!Character.isLetter(work.charAt(origin))) {
    		        				origin--;
    		        			}else {
    		        				break;
    		        			}       		
    		        		}
    	            		if(origin==0)continue;
    	            		else {
    	            			String res = work.substring(0, origin+1);
    	            			System.out.println(res+"++");
    	            		} 				
    		        }
    			}
    			
    	
//    		StringTokenizer st = new StringTokenizer(value.toString());
//    		while(st.hasMoreTokens()){	
//				String word = st.nextToken();
//				System.out.println(word+"++");
//    		}
    	
    	
//        String line = value.toString();
//        String []strArray=line.split(";");
//        for( int i = 0; i<strArray.length; i++) {
//        		String temp  = strArray[i];
//        		while(temp.length()>0) {
//        			if(temp.charAt(temp.length()-1)!=' ') {
//        				break;
//        			}else {
//        				temp = temp.substring(0, temp.length()-1);
//        			}       		
//        		}
//        		if(temp.length()==0)continue;
//        		System.out.println(temp.length());
//        }
        
    }
}

     
    
public static void main(String[] args) throws Exception {

         Configuration conf = new Configuration();

     
        Job job = Job.getInstance(conf);
    
        job.setJarByClass(Test0.class);
        job.setJobName("myjob");
        
    
        job.setMapperClass(MyMapper.class);
    
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
    
        job.waitForCompletion(true);
        

        
    }

}
