package indexSearchAssignment1;



import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;


public class index {

	public static void main(String[] args) throws IOException, ParseException {
		// preparing to index
		Analyzer analyzer =  new SimpleAnalyzer(Version.LUCENE_46); // simple tokenization
		Directory directory = new RAMDirectory(); // store the index in main memory
		IndexWriterConfig config = new IndexWriterConfig(Version.LUCENE_46,analyzer);
		IndexWriter writer = new IndexWriter(directory, config);
		
		FieldType ft=new FieldType();
		ft.setTokenized(true);
		ft.setIndexed(true);
		ft.setStored(false);
		ft.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS);
	 	    
	 	// index a document collection
	 	File[] files = new File("./collection").listFiles();
	 	int id = 0;
	 	for (File f: files) {
		 	Document doc = new Document();
		 	String docid = "doc" + id;
			doc.add(new Field("content", new FileReader(f), ft));
			doc.add(new StringField("ID", docid, Field.Store.YES));
		 	writer.addDocument(doc);
		 	System.out.println("Indexing... " + docid);
		 	id++;
	 	}
	 	
		// done with indexing
	 	writer.close();
		System.out.println("Finishing indexing " + id + " documents.");
		System.out.println();
		    
	    
	    //prepare for retrieving...
	    IndexReader reader = DirectoryReader.open(directory);
	    IndexSearcher searcher = new IndexSearcher(reader);

	    //print out information in inverted index
	    System.out.println("Inverted index: " + reader.numDocs() + 
	    		" documents and " + getDistinctTerm(reader, "content") + " distinct terms");
	    showPosting(reader, "content"); // show posting list of each term
	    
	    
	    // document ranking
	    QueryParser parser = new QueryParser(Version.LUCENE_46, "content", analyzer);
	    Query query = parser.parse("I am you. You be I.");
	    TopDocs hits = searcher.search(query, 5); // similarity is based on Lucene conceptual scoring
	    System.out.println("Document score:");
	    for(ScoreDoc scoreDoc : hits.scoreDocs) {
			Document doc = searcher.doc(scoreDoc.doc);
			System.out.println(doc.get("ID") + ": " + scoreDoc.score);
		}
	
	    // done!
	    reader.close();
	    directory.close();
	    
	}
	
	// Return the number of distinct terms in an index reader
	public static long getDistinctTerm(IndexReader reader, String searchfield) throws IOException {
		long numTerm = 0;
		
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
		
		return numTerm;
	}
	
	// show posting lists
	public static void showPosting(IndexReader reader, String searchfield) throws IOException {
		
		Fields fields=MultiFields.getFields(reader);
		System.out.printf("%-15s%-15s%-15s\n", "term", "doc freq", "Posting (ID:TF)");
		System.out.printf("%-15s%-15s%-15s\n", "----", "--------", "---------------");
		Bits livedocs=MultiFields.getLiveDocs(reader);
		for(String field:fields){
			if(field.equals(searchfield)){
				Terms terms = fields.terms(searchfield);
				TermsEnum termsEnum = terms.iterator(null);
				BytesRef text=null;
				while((text=termsEnum.next())!=null){
					String term=text.utf8ToString();
					int docfreq=termsEnum.docFreq();
					System.out.printf("%-15s%-15d", term, docfreq);
					
					DocsEnum posting = null;
					posting=termsEnum.docs(livedocs, posting);
					
					while (posting.nextDoc()!=DocsEnum.NO_MORE_DOCS) {
						System.out.printf("%d:%d, ", posting.docID(), posting.freq());
					}
					System.out.println("end");
					
					
				}
			}
		}
		System.out.println();
	}
}

