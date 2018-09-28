package Top_K;

import java.io.IOException;
 
/**
 * 
 * @author zx
 *zhangxian1991@qq.com
 */
public class TopK {
	public static void main(String args[]) throws ClassNotFoundException, IOException, InterruptedException{
		
		if(args.length < 5){
			throw new IllegalArgumentException("要有5个参数:1，要统计的文本文件。2，统计后的结果。3，排序后的结果。4，前k个词存放的结果路径。5，k");
		}
		
		//要统计字数的文本文件名
		String in = args[0];
		
		//统计字数后的结果
		String wordCout = args[1];
 
		in = FileUtil.loadFile(wordCout, "TopK", in);

		//对统计完后的结果再排序后的内容
		String sort = args[2];
		
		//前K条
		String topK = args[3];
		
		int k = Integer.parseInt(args[4]);
		System.out.println(in);
		System.out.println(sort);
		System.out.println(topK);
		//如果统计字数的job完成后就开始排序
		if(WordCount.run(in, wordCout)){
			Sort.run(wordCout, sort, topK, k);
		}
		
	}
}

