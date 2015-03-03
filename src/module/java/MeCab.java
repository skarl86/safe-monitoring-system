/**
 * Created by NCri on 15. 2. 27..
 */
// JAVA SDK
import java.util.*;

// MECAB SDK
import org.chasen.mecab.Node;
import org.chasen.mecab.Tagger;

public class MeCab {
    static{
        System.load(System.getProperty("java.library.path") + "/libMeCab.so");
    }

    private String _wordClass = "(.*NNG.*|.*NNP.*|.*NNB.*|.*NR.*|.*NP.*|.*SL.*)";//|.*SN.*)";// |.*VV.*|.*VA.*|.*MAG.*|.*XR.*)";
    private String _unnecessaryWordClass = "(.*SF.*|.*SE.*|.*SSO.*|.*SSC.*|.*SC.*|.*SY.*)";
    private Tagger tagger = new Tagger("-d /usr/local/lib/mecab/dic/mecab-ko-dic");

    private Node root = null;
    private Node nextNode = null;
    private String key = null;
    private String feature = null;

    String parseTweet(String line){ return line.split("\t")[2]; }

    List<List<String>> parseWord(List<String> lines){
        List<List<String>> wordList = new ArrayList<List<String>>();
        List<String> tempList = null;

        for(String line : lines){
            tempList = new ArrayList<String>();
            line = parseTweet(line);

            root = tagger.parseToNode(line);
            nextNode = root;
            while (nextNode != null) {
                key = nextNode.getSurface();
                feature = nextNode.getFeature().split(",")[0];
                if (feature.matches(_wordClass)){
                    // 명사 이외에 품사를 제외한 단어를 추출.
                    // 중복 단어 없이.
                    if(key.length() >= 1)
                        tempList.add(key);
                }

                nextNode = nextNode.getNext();
            }
            wordList.add(tempList);

        }
        return wordList;
    }
    List<String> parseWord(String rowLine) {
        // TODO Auto-generated method stub
        List<String> wordList = new ArrayList<String>();

        // 형태소 분석 결과.
        String temp = tagger.parse(rowLine);
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
                    wordList.add(key);
                }
            }
        }

//        root = tagger.parseToNode(line);
//        nextNode = root;
//        while (nextNode != null) {
//            key = nextNode.getSurface();
//            feature = nextNode.getFeature().split(",")[0];
//            if (feature.matches(_wordClass)){
//                // 명사 이외에 품사를 제외한 단어를 추출.
//                // 중복 단어 없이.
//                if(key.length() >= 1)
//                    wordList.add(key);
//            }
//
//            nextNode = nextNode.getNext();
//        }
        return wordList;
    }
}
