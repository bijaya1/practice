package sorting;

public class SelectionSort {

	
	public static void main(String [] args){
		int [] inputArray = {131, 491, 100, 132, 467, 544,101};
	    int ArrayLength = inputArray.length;
	for(int k=0;k<ArrayLength-1;k++){	
		for (int i=0;i<ArrayLength-1;i++){
		if (inputArray[i] <inputArray[i+1]){
//			Idea here is to swap two adjacent integer.
			int tmp = inputArray[i];
			inputArray[i] = inputArray[i+1];
			inputArray[i+1]=tmp;
		}
		
		}
	
		}
	for (int j=0; j<ArrayLength;j++){
	System.out.println(inputArray[j]);
	}
	
	}
}
