import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class CSE535Assignment {

	/**
	 * 
	 * @param args
	 *            4 command line arguments args[0] - index file args[1] - log
	 *            file args[2] - K args[3] - query file
	 */
	public static void main(String[] args) {

		if (args.length != 4) {
			System.err.println("Usage: java CSE535Assignment <index_file> <log_file> <K> <query_file>");
			return;
		}

		Index docIdOrderedIndex = new DocIdOrderedIndex(args[0]);
		Index tfOrderedIndex = new TermFreqOrderedIndex(args[0]);

		outputToLog(docIdOrderedIndex, tfOrderedIndex, args);
	}

	public static void outputToLog(Index docIdOrderedIndex, Index tfOrderedIndex, String... args) {

		String lineSeparator = System.lineSeparator();

		try (BufferedWriter bw = new BufferedWriter(new FileWriter(args[1]))) {

			// Top K terms
			bw.write("FUNCTION: getTopK " + args[2] + lineSeparator);
			bw.write("Result:");
			List<Term> termList = docIdOrderedIndex.getTopK(Integer.parseInt(args[2]));
			int i = 0;
			for (Term t : termList) {
				bw.write(" " + t.getTermStr());
				if (i != termList.size() - 1) {
					bw.write(",");
				}
				++i;
			}
			bw.write(lineSeparator);

			// postings for the query terms
			try (BufferedReader br = new BufferedReader(new FileReader(args[3]))) {
				String line;
				while ((line = br.readLine()) != null) {
					String[] queryTerms = line.split(" ");
					for (String queryTerm : queryTerms) {
						bw.write("FUNCTION: getPostings " + queryTerm + lineSeparator);
						Term qt = new Term(queryTerm, 0);
						List<Posting> docIndexPL = docIdOrderedIndex.getPostings(qt);
						List<Posting> tfIndexPl = tfOrderedIndex.getPostings(qt);
						if (docIndexPL != null) {
							bw.write("Ordered by doc IDs:");
							i = 0;
							for (Posting p : docIndexPL) {
								bw.write(" " + p.getId());
								if (i != docIndexPL.size() - 1) {
									bw.write(",");
								}
								++i;
							}
							bw.write(lineSeparator);

							bw.write("Ordered by TF:");
							i = 0;
							for (Posting p : tfIndexPl) {
								bw.write(" " + p.getId());
								if (i != tfIndexPl.size() - 1) {
									bw.write(",");
								}
								++i;
							}
							bw.write(lineSeparator);
						} else {
							bw.write("term not found" + lineSeparator);
						}

					}
				}
			} catch (IOException ioe) {
				System.err.println("Error while reading the query file");
			}

		} catch (IOException e) {
			System.err.println("Error writing result to log file");
			e.printStackTrace();
		}
	}

}

class Posting {
	private int id;
	private int termFreq;

	public Posting(int id, int termFreq) {
		this.id = id;
		this.termFreq = termFreq;
	}

	public int getId() {
		return id;
	}

	public int getTermFreq() {
		return termFreq;
	}
}

class Term {
	private String termStr;
	private int postingSize;

	public Term(String termStr, int postingSize) {
		this.termStr = termStr;
		this.postingSize = postingSize;
	}

	public String getTermStr() {
		return termStr;
	}

	public int getPostingSize() {
		return postingSize;
	}

	@Override
	public boolean equals(Object obj) {
		Term otherTerm = (Term) obj;
		if (otherTerm != null) {
			if (this.getTermStr().equals(otherTerm.getTermStr())) {
				return true;
			}
		}
		return false;
	}

	@Override
	public int hashCode() {
		return getTermStr().hashCode();
	}

}

class QueryResult {
	int numOfDocs;
	int numOfComparisons;
	int runTime; // in seconds
	List<Integer> docIds;

	public QueryResult(int numOfDocs, int numOfComparisons, int runTime, List<Integer> docIds) {
		this.numOfDocs = numOfDocs;
		this.numOfComparisons = numOfComparisons;
		this.runTime = runTime;
		this.docIds = docIds;
	}
}

interface Index {
	public List<Term> getTopK(int k);

	public List<Posting> getPostings(Term term);

	public String termAtATimeQueryAnd();

	public String termAtATimeQueryOr();

	public String docAtATimeQueryAnd();

	public String docAtATimeQueryOr();
}

class DocIdOrderedIndex implements Index {

	private Map<Term, List<Posting>> idx;

	public DocIdOrderedIndex(String indexFileName) {
		idx = new HashMap<Term, List<Posting>>();

		parseIndexFile(indexFileName);
	}

	private void parseIndexFile(String indexFileName) {
		try (BufferedReader br = new BufferedReader(new FileReader(indexFileName))) {
			String line;
			while ((line = br.readLine()) != null) {
				int cIndex = line.indexOf("\\c");
				int mIndex = line.indexOf("\\m");

				int plSize = Integer.parseInt(line.substring(cIndex + 2, mIndex));
				Term term = new Term(line.substring(0, cIndex), plSize);
				List<Posting> pList = new ArrayList<Posting>(plSize);
				String pListString = line.substring(mIndex + 3, line.length() - 1);
				String[] postings = pListString.split(", ");
				for (String posting : postings) {
					String[] vals = posting.split("/");
					// input is anyway sorted in increasing order of doc id's
					pList.add(new Posting(Integer.parseInt(vals[0]), Integer.parseInt(vals[1])));
				}

				idx.put(term, pList);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Index file \"" + indexFileName + "\"" + " not found");
		} catch (IOException e) {
			System.err.println("Error while reading the index file");
			e.printStackTrace();
		}
	}

	@Override
	public List<Term> getTopK(int k) {
		
		List<Term> allTerms = new ArrayList<Term>(idx.size());
		allTerms.addAll(idx.keySet());
		Collections.sort(allTerms, new Comparator<Term>() {

			@Override
			public int compare(Term t1, Term t2) {
				if (t1.getPostingSize() > t2.getPostingSize()) {
					return -1;
				} else if (t1.getPostingSize() < t2.getPostingSize()) {
					return 1;
				}
				return 0;
			}
		});
		
		List<Term> outputList = new ArrayList<Term>(k);
		for (Term term : allTerms) {
			if (k == 0) {
				break;
			}
			outputList.add(term);
			--k;
		}
		return outputList;
	}

	@Override
	public List<Posting> getPostings(Term term) {
		return idx.get(term);
	}

	@Override
	public String termAtATimeQueryAnd() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String termAtATimeQueryOr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String docAtATimeQueryAnd() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String docAtATimeQueryOr() {
		// TODO Auto-generated method stub
		return null;
	}

}

class TermFreqOrderedIndex implements Index {

	private Map<Term, List<Posting>> idx;

	public TermFreqOrderedIndex(String indexFileName) {
		idx = new HashMap<Term, List<Posting>>();
		parseIndexFile(indexFileName);
	}
	
	private void parseIndexFile(String indexFileName) {
		try (BufferedReader br = new BufferedReader(new FileReader(indexFileName))) {
			String line;
			while ((line = br.readLine()) != null) {
				int cIndex = line.indexOf("\\c");
				int mIndex = line.indexOf("\\m");

				int plSize = Integer.parseInt(line.substring(cIndex + 2, mIndex));
				Term term = new Term(line.substring(0, cIndex), plSize);
				String pListString = line.substring(mIndex + 3, line.length() - 1);
				String[] postings = pListString.split(", ");
				List<Posting> pList = new ArrayList<Posting>(plSize);
				for (String posting : postings) {
					String[] vals = posting.split("/");
					
					int docId = Integer.parseInt(vals[0]);
					int tf = Integer.parseInt(vals[1]);
					Posting p = new Posting(docId, tf);
					int i = 0;
					do {
						if (pList.isEmpty()) {
							pList.add(p);
							break;
						} else if (pList.get(i).getId() > docId){
							++i;
						} else {
							pList.add(i, p);
							break;
						}
					} while(i < pList.size());
				}
				idx.put(term, pList);
			}
		} catch (FileNotFoundException e) {
			System.err.println("Index file \"" + indexFileName + "\"" + " not found");
		} catch (IOException e) {
			System.err.println("Error while reading the index file");
			e.printStackTrace();
		}
	}

	@Override
	public List<Term> getTopK(int k) {
		// not required
		// since this method will be invoked on the other type of index
		return null;
	}

	@Override
	public List<Posting> getPostings(Term term) {
		return idx.get(term);
	}

	@Override
	public String termAtATimeQueryAnd() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String termAtATimeQueryOr() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String docAtATimeQueryAnd() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String docAtATimeQueryOr() {
		// TODO Auto-generated method stub
		return null;
	}

}