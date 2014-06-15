package indexSearchAssignment1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;


public class indexSearch {
	public static Analyzer analyzer =  new EnglishAnalyzer(Version.LUCENE_46); 
	public static Directory directory = new RAMDirectory(); // store the index in main memory
	public static IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46,analyzer);
	public static QueryParser parser = new QueryParser(Version.LUCENE_46, "content", analyzer);
	public static IndexWriter writer;
	public static IndexReader reader;
    public static IndexSearcher searcher;
    public static ArrayList l;
    public static int doc_ID;

    public static void main(String[] args) throws IOException, ParseException{
	 	writer = new IndexWriter(directory, config);
	 	readFile(writer);
	 	
	 	reader= DirectoryReader.open(directory);
	 	searcher= new IndexSearcher(reader);
		getDistinctTerm(reader, "content");
		
		getFrequentTerm(reader,"content");
		
		String []s={"lymphoid","cells"};
		retrieveDocID(searcher,s);
		
//		Term term1=new Term("content","lymphoid");
//		Term term2=new Term("content","cell");
//		retrievePhraseQueryDocID(searcher,term1,term2);
		String s1="lymphoid cells";
		retrievePhraseQueryDocID(searcher,s1);
		for(int i=0;i<l.size();i++){
			System.out.println(l.get(i));
		}
		System.out.println("");
		
		getPosition(reader,"lymphoid cells");
		
		reader.close();
		directory.close();
	}
	
	private static void readFile(IndexWriter writer) throws IOException{
		File file=new File("./MED.ALL");//	
		FieldType ft=new FieldType();
		BufferedReader in=new BufferedReader(new FileReader(file));
		String fileContent="";
		String docID="";
		String cache=""; //store the file by temporary
		int flag=0;		
		ft.setIndexed(true);
		ft.setStored(false);
		ft.setStoreTermVectorPositions(true);
		ft.setStoreTermVectors(true);
		ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);//	
        while ((fileContent = in.readLine()) != null){
        	if(fileContent.matches(".I [0-9]{1,}")){
        		if(flag==0){
        			flag++;
        			docID=fileContent;
        		}
        		else{		
        			Document doc=new Document();
            		doc.add(new Field("content",cache,ft));
            		doc.add(new StringField("docID",docID,Field.Store.YES));
            		writer.addDocument(doc);
            		docID=fileContent;       		
            		cache="";
        		}        		
        	}
        	else if(fileContent.matches(".W")){
        		continue;
        	}
        	else{
        		cache+=fileContent+"\r\n"; //act as cache to store the string line of file temporarily
        	}
        }
        Document doc=new Document();
        doc.add(new Field("content",cache,ft));
        doc.add(new StringField("docID",docID,Field.Store.YES));
        writer.addDocument(doc);
        in.close();
        writer.close();
	}
	
	private static void getDistinctTerm(IndexReader reader, String searchfield) throws IOException{
		long numTerm=0;		
		Fields fields=MultiFields.getFields(reader);
		for(String field:fields){
			if(field.equals(searchfield)){
				Terms terms = fields.terms(searchfield);
				TermsEnum termsEnum = terms.iterator(null);
				BytesRef text=null;
				while((text=termsEnum.next())!=null){
					numTerm++;
				}
			}
		}
		System.out.println("The number of distinct terms is "+numTerm+".");
		System.out.println("");
	}
	
	private static void getFrequentTerm(IndexReader reader, String searchfield) throws IOException{
		Fields fields=MultiFields.getFields(reader);
		String term="";
		int termFreq=0;
		Map<String,Integer> freqTerm=new HashMap<String,Integer>();
		ArrayList arrayList=null;		
		for(String field:fields){
			if(field.equals(searchfield)){
				Terms terms = fields.terms(searchfield);
				TermsEnum termsEnum = terms.iterator(null);
				BytesRef text=null;
				while((text=termsEnum.next())!=null){
					term=text.utf8ToString();
					termFreq=termsEnum.docFreq();
					freqTerm.put(term, termFreq);
				}
			}
		}		
		arrayList=new ArrayList(freqTerm.entrySet());
		Collections.sort(arrayList, new Comparator(){
			@Override
			public int compare(Object arg0, Object arg1) {
				// TODO Auto-generated method stub
				Map.Entry obj1 = (Map.Entry) arg0;  
                Map.Entry obj2 = (Map.Entry) arg1;  
                return ((Integer) obj2.getValue()).compareTo((Integer)obj1.getValue()); 
			}
		});
    	System.out.println("The most frequent terms are "+arrayList.get(0)+" and "+arrayList.get(1));
    	System.out.println("");
	}
	
	private static void retrieveDocID(IndexSearcher searcher,String []s) throws ParseException, IOException{
		BooleanQuery bQuery = new BooleanQuery();
		for (int i=0;i<s.length;i++){
			Query query=parser.parse(s[i]);
			bQuery.add(query, Occur.MUST);
		}
		TopDocs hits = searcher.search(bQuery, 5); // similarity is based on Lucene conceptual scoring
		System.out.println("The top 5 documents which contain 'lymphoid' and 'cells' are:");
		for(ScoreDoc scoreDoc : hits.scoreDocs) {
			Document doc = searcher.doc(scoreDoc.doc);
			System.out.println(doc.get("docID") + ": " + scoreDoc.score);
		}
		System.out.println("");
	}
	
	private static void retrievePhraseQueryDocID(IndexSearcher seacher,String s) throws IOException{
		PhraseQuery pQuery=new PhraseQuery();
		Map<String, Float> docList=new HashMap<String, Float>();
		int count=0;
		TokenStream tStream=analyzer.tokenStream("content", s);
		tStream.reset();
		CharTermAttribute attribute=tStream.addAttribute(CharTermAttribute.class);	
		while(tStream.incrementToken()){
			pQuery.add(new Term("content",attribute.toString()),count);
			count++;
		}
//		pQuery.add(t1);
//		pQuery.add(t2);
//		pQuery.setSlop(0);
		TopDocs hits=searcher.search(pQuery,reader.maxDoc());
		System.out.println("The documents which contain the phrase 'lymphoid cells' are:");
		for(ScoreDoc scoreDoc : hits.scoreDocs) {
			doc_ID =scoreDoc.doc;
			Document doc = searcher.doc(doc_ID);
			docList.put(doc.get("docID"), scoreDoc.score);
		}
//		for(Object o: docList.keySet() ){
//			Object key=o;
//			Object value = docList.get(o);
//			System.out.println(value);
//		}
		l=new ArrayList(docList.entrySet());
		Collections.sort(l, new Comparator(){
			@Override
			public int compare(Object o1, Object o2) {
				// TODO Auto-generated method stub
				Map.Entry obj1 = (Map.Entry) o1;  
                Map.Entry obj2 = (Map.Entry) o2;  
                if(obj1.getKey().toString().length()<obj2.getKey().toString().length()){
                	return -1;
                }
                else if(obj1.getKey().toString().length()==obj2.getKey().toString().length()){
                	if(obj1.getKey().toString().compareTo(obj2.getKey().toString())<0){
                		return -1;
                	}
                }
                return 1;
			}
		});
		tStream.close();
	}
	
	private static void getPosition(IndexReader reader,String s) throws IOException{
		String id="";
		int documentID;
		int index;
		for(int i=0;i<l.size();i++){
			Integer []position1=new Integer[10000];
			Integer []position2=new Integer[10000];
			int m=0,n=0,count=0;
			String []queryString=new String[20];
			index=l.get(i).toString().indexOf("=");
			id=l.get(i).toString().substring(3, index);
			documentID=Integer.parseInt(id);
			System.out.print(".I "+documentID+": ");
			
			Terms terms=reader.getTermVector(documentID-1, "content");
			TermsEnum termsEnum=terms.iterator(null);	
			
			TokenStream tStream=analyzer.tokenStream("content", s);
			tStream.reset();
			CharTermAttribute attribute=tStream.addAttribute(CharTermAttribute.class);	
			while(tStream.incrementToken()){
				queryString[count]=attribute.toString();
				count++;
			}
			tStream.close();
			
			BytesRef text=null;
			while((text=termsEnum.next())!=null){
				int freq1=0,freq2=0;
				String term=text.utf8ToString();
				if(term.equals(queryString[0])==true){
					DocsAndPositionsEnum docPosEnum = termsEnum.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_PAYLOADS);
					docPosEnum.nextDoc();
					freq1 = docPosEnum.freq();
					for(int j=0;j<freq1;j++){
						position1[j]=docPosEnum.nextPosition();
//						System.out.print(position2[j]+"->");
					}
//					System.out.println("end");
				}
				if(term.equals(queryString[1])==true){
					DocsAndPositionsEnum docPosEnum = termsEnum.docsAndPositions(null, null, DocsAndPositionsEnum.FLAG_OFFSETS);
					docPosEnum.nextDoc();
					freq2 = docPosEnum.freq();
					for(int j=0;j<freq2;j++){
						position2[j]=docPosEnum.nextPosition();
//						System.out.print(position1[j]+"->");
					}
//					System.out.println("end");
				}
			}
			while(position1[m]!=null && position2[n]!=null){
				if(position1[m]<position2[n]){
					if(position2[n]-position1[m]==1){
						System.out.print((position1[m]+1)+",");
						m++;
						n++;
						continue;
					}
					else{
						m++;
						continue;
					}
				}
				else{
					n++;
					continue;
				}
			}
			System.out.println("end");
		}
	}
}
