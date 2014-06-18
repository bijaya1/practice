package sorting;
//  yo insertion sort hoina need to check it again.
public class insertionSort {
	
	public static void main(String [] args){
	int [] inputArray = {131, 491, 100, 132, 467, 544,101};
    int ArrayLength = inputArray.length;
    int maxNum, pos =0, tmp2;

    
    for(int k=0; k<ArrayLength;k++){
    	tmp2 = maxNum = inputArray[k];
    	boolean swap =false;
    	for (int j=k; j<ArrayLength;j++){
    		if(inputArray[j]>maxNum){
    			maxNum= inputArray[j];
    			pos = j; //we need to remember the position that we want to swap	
    		swap = true; //check if swap occur. If swap did not occur no need to update the array.
    		}
    		
    	}
    	if(swap){
    	
    	inputArray[k]=maxNum;
    	inputArray[pos] = tmp2;

    	}
//        System.out.println();
//    for (int i=0; i<ArrayLength;i++){
//    	System.out.print(" "+inputArray[i]);
    }

    for (int i=0; i<ArrayLength;i++){
	System.out.print(" "+inputArray[i]);
	}
}
}
