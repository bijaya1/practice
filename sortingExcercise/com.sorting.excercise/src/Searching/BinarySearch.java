package Searching;

public class BinarySearch {

	
	
	public static void main(String [] args){
		int findInteger = Integer.parseInt(args[0]);
		int[] inputArray = {13,	24,	34,	46,	52,	63,	77,	89,	91,	100};
		int startIndex =0;
		int endIndex =inputArray.length;
		
//		for (int i=0;i<endIndex;i++){
//		System.out.println(inputArray[i]);
//		}
		int mid_point_Integer=(endIndex+startIndex)/2;
		int i =0;
		while (startIndex != endIndex){
//		 find input integer in the array using binary sort.
	    mid_point_Integer=(endIndex+startIndex)/2;
//		check condition where does input integer belong to left or right
		
		if (inputArray[mid_point_Integer] == findInteger){
			System.out.println("your number is in "+ mid_point_Integer + "position");
			System.out.println(startIndex+" "+endIndex);
			System.exit(0);
		}if (inputArray[mid_point_Integer] < findInteger){
			startIndex=mid_point_Integer+1;
			
	
			System.out.println("less than find value"+startIndex+" "+endIndex);
		}else{
			
			endIndex =mid_point_Integer-1;
		System.out.println("greater than find value"+startIndex+" "+endIndex);
		}
	i++;	
	System.out.println("i=>"+i);
	}
		System.out.println("Finding"+findInteger+".....Integer doesnot exist");
	}
}
