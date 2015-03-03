/**
 * Created by NCri on 15. 2. 27..
 */
// JAVA SDK
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

// MECAB SDK
import org.chasen.mecab.Node;
import org.chasen.mecab.Tagger;

public class MeCab {
    static{
        System.load(System.getProperty("java.library.path") + "/libMeCab.so");
    }

    private String _wordClass = "(.*NNG.*|.*NNP.*|.*NNB.*|.*NR.*|.*NP.*|.*SL.*)";//|.*SN.*)";// |.*VV.*|.*VA.*|.*MAG.*|.*XR.*)";
    private Tagger _tagger = new Tagger("-d /usr/local/lib/mecab/dic/mecab-ko-dic");
    private ArrayList<String> _stopWords = new ArrayList<String>();

    public MeCab(){
        // 불용사전 읽어오기.
        BufferedReader br = null;
        String stopword = null;

        try {
            br = new BufferedReader(new FileReader("./dic/stopword.txt"));
            stopword = br.readLine();
            for(String word : stopword.split(",")) {
                _stopWords.add(word);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    List<String> parseWord(String rowLine) {
        // TODO Auto-generated method stub

        List<String> wordList = new ArrayList<String>();

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
                if(feature.matches(_wordClass)){
                    // 불용어 처리.
                    if(!_stopWords.contains(key))
                        wordList.add(key);
                }
            }
        }

        return wordList;
    }
}
