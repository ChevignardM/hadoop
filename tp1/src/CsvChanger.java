import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;

public class CsvChanger {


		private static String[][] tabs;
		private static String[][] pivots;

		public static void main(String[] args){

	        String csvFile = "C:\\Users\\Maxime\\workspace\\TP Hadoop\\fichier.csv";
	        String item = "";
	        String cvsSplitBy = ",";
	        int ligne =1;
	      tabs = null;
	        pivots = null;
	        
	      

	        //lecture csv
	        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {
	        	

	            while ((item = br.readLine()) != null) {

	                String[] line = item.split(cvsSplitBy);
	                tabs[ligne] = line;
	                ligne ++;
	            }
	            
	            //pivot
		        
		        for(int i=0; i<tabs.length; i++){
		        	for (int j=0; j<tabs[i].length; j++){
		        		pivots[i][j] = tabs[j][i];
		        	}
		        }

	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	        
	        //écriture csv
	        
	        PrintWriter pw = null;
			try {
				pw = new PrintWriter(new File("C:\\Users\\Maxime\\workspace\\TP Hadoop\\output.csv"));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        StringBuilder sb = new StringBuilder();
	        for(int l=0; l<pivots.length; l++){
	        sb.append(pivots[l]);
	        sb.append('\n');
	        }

	        pw.write(sb.toString());
	        pw.close();
	        
		}

}
