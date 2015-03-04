/**
 * Created by NCri on 15. 2. 27..
 */
// JAVA SDK
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.sql.*;


// MECAB SDK
import org.chasen.mecab.Node;
import org.chasen.mecab.Tagger;
import scala.util.parsing.combinator.testing.Str;

public class MeCab {
    static{
        System.load(System.getProperty("java.library.path") + "/libMeCab.so");
    }

    private String _wordClass = "(.*NNG.*|.*NNP.*|.*NNB.*|.*NR.*|.*NP.*|.*SL.*)";//|.*SN.*)";// |.*VV.*|.*VA.*|.*MAG.*|.*XR.*)";
    private Tagger _tagger = new Tagger("-d /usr/local/lib/mecab/dic/mecab-ko-dic");
    private List<String> _stopwordList = null;

    List<String> parseWord(String rowLine) {
        // TODO Auto-generated method stub

        List<String> wordList = new ArrayList<String>();

        if(_stopwordList == null) {
            _stopwordList = getStopWord();
        }

        // 형태소 분석 결과.
        String temp = _tagger.parse(rowLine);
        // 형태소 분석 결과를 라인 단위로 짜름.
        String[] lines = temp.split("\n");

        // 품사.
        String feature = null;
        // 단어.
        String key = null;

        for(String line : lines){
            // 끝을 의미하는 EOS는 제외.
            if(!line.equals("EOS")){
                key = line.split("\t")[0];
                feature = line.split("\t")[1].split(",")[0];
                if(key.length() > 1){
                    if(feature.matches(_wordClass)){
                        // 불용어 처리.
                        if(!_stopwordList.contains(key))
                            wordList.add(key);
                    }
                }
            }
        }

        return wordList;
    }
    List<String> getStopWord(){
        ArrayList<String> words = new ArrayList<String >();
        BufferedReader br = null;
        String line = null;
        try{
            br = new BufferedReader(new FileReader("./dic/stopword.txt"));
            line = br.readLine();
            for(String word : line.split(",")){
                words.add(word);
            }
            br.close();
        }catch(Exception e){
            e.printStackTrace();
        }

        return words;



//        try
//        {
//            // create our mysql database connection
//            String myDriver = "org.gjt.mm.mysql.Driver";
//            String myUrl = "jdbc:mysql://218.54.47.24:3306/tweet";
//            Class.forName(myDriver);
//            Connection conn = DriverManager.getConnection(myUrl, "root", "tkfkdgo1_");
//
//            // our SQL SELECT query.
//            // if you only need a few columns, specify them by name instead of using "*"
//            String query = "SELECT stopword FROM stopwordtable";
//
//            // create the java statement
//            Statement st = conn.createStatement();
//
//            // execute the query, and get a java resultset
//            ResultSet rs = st.executeQuery(query);
//
//            // iterate through the java resultset
//            while (rs.next())
//            {
//                String stopword = rs.getString("stopword");
//                words.add(stopword);
//            }
//            st.close();
//        }
//        catch (Exception e)
//        {
//            System.err.println("Got an exception! ");
//            System.err.println(e.getMessage());
//        }
//        return words;
    }
}
