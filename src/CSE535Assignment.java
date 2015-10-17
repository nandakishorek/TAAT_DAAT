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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class CSE535Assignment {

    /**
     * Name - Nandakishore Krishna
     * UBIT - NANDAKIS
     * email - NANDAKIS@BUFFALO.EDU
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

        // read the index into mem
        Index docIdOrderedIndex = new DocIdOrderedIndex(args[0]); // for DAAT strategy
        Index tfOrderedIndex = new TermFreqOrderedIndex(args[0]); // for TAAT strategy

        outputToLog(docIdOrderedIndex, tfOrderedIndex, args);
    }

    /**
     *  Method to print the output of the computations to the log file
     *  
     * @param docIdOrderedIndex
     * @param tfOrderedIndex
     * @param args
     */
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

            try (BufferedReader br = new BufferedReader(new FileReader(args[3]))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] queryTerms = line.split(" ");

                    // postings for the query terms
                    for (String queryTerm : queryTerms) {
                        bw.write("FUNCTION: getPostings " + queryTerm + lineSeparator);
                        Term qt = new Term(queryTerm, 0);
                        // get the postings list for the query term
                        List<Posting> docIndexPL = docIdOrderedIndex.getPostings(qt);
                        List<Posting> tfIndexPl = tfOrderedIndex.getPostings(qt);
                        
                        // output the postings list only if the term is found
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

                    List<Term> queryTermList = new ArrayList<Term>(queryTerms.length);
                    for (String queryTerm : queryTerms) {
                        queryTermList.add(new Term(queryTerm, 0));
                    }

                    String commaSepQueryTerms = toCommaSeparatedString(queryTerms);

                    // termAtATimeQueryAnd
                    bw.write("termAtATimeQueryAnd " + commaSepQueryTerms + lineSeparator);
                    QueryResult queryResult = tfOrderedIndex.conjuctiveQuery(queryTermList, false);
                    QueryResult queryResultWtOptzn = tfOrderedIndex.conjuctiveQuery(queryTermList, true);
                    printQueryResult(queryResult, queryResultWtOptzn.getNumCompWtOptimzn(), bw);

                    // termAtATimeQueryOr
                    bw.write("termAtATimeQueryOr " + commaSepQueryTerms + lineSeparator);
                    queryResult = tfOrderedIndex.disjunctiveQuery(queryTermList, false);
                    queryResultWtOptzn = tfOrderedIndex.disjunctiveQuery(queryTermList, true);
                    printQueryResult(queryResult, queryResultWtOptzn.getNumCompWtOptimzn(), bw);

                    // docAtATimeQueryAnd
                    bw.write("docAtATimeQueryAnd " + commaSepQueryTerms + lineSeparator);
                    queryResult = docIdOrderedIndex.conjuctiveQuery(queryTermList, false);
                    printQueryResult(queryResult, -1, bw);

                    // docAtATimeQueryOr
                    bw.write("docAtATimeQueryOr " + commaSepQueryTerms + lineSeparator);
                    queryResult = docIdOrderedIndex.disjunctiveQuery(queryTermList, false);
                    printQueryResult(queryResult, -1, bw);

                }
            } catch (IOException ioe) {
                System.err.println("Error while reading the query file");
            }

        } catch (IOException e) {
            System.err.println("Error writing result to log file");
            e.printStackTrace();
        }
    }

    
    /**
     * Method to print the result of the query - could be AND or OR
     *  
     * @param queryResult   result of the query
     * @param numCompOptimzn   the number of comparisons when optimization is turned on
     * @param bw   handle to the output file writer
     * @throws IOException
     */
    public static void printQueryResult(QueryResult queryResult, int numCompOptimzn, BufferedWriter bw)
            throws IOException {
        String lineSeparator = System.lineSeparator();
        List<Integer> docIdList = queryResult.getDocIds(); 
        if (docIdList == null) {
            // none of the terms were found
            bw.write("terms not found" + lineSeparator);
        } else {
            bw.write(docIdList.size() + " documents are found" + lineSeparator);
            bw.write(queryResult.getNumOfComparisons() + " comparisions are made" + lineSeparator);
            bw.write(queryResult.getRunTime() + " seconds are used" + lineSeparator);
            if (numCompOptimzn > -1) {
                bw.write(numCompOptimzn + " comparisons are made with optimization (optional bonus part)"
                        + lineSeparator);
            }

            Collections.sort(docIdList);
            String[] docIds = new String[queryResult.getDocIds().size()];
            for (int i = 0; i < docIds.length; ++i) {
                docIds[i] = docIdList.get(i).toString();
            }

            bw.write("Result " + toCommaSeparatedString(docIds) + lineSeparator);
        }
    }

    /**
     * Method to convert the string array to a comma separated string
     * 
     * @param strArray
     * @return
     */
    public static String toCommaSeparatedString(String[] strArray) {
        StringBuilder sb = new StringBuilder();
        int i = 0;
        for (String str : strArray) {
            sb.append(str);
            
            // do not append comma in the end
            if (i != strArray.length - 1) {
                sb.append(", ");
            }
            ++i;
        }
        return sb.toString();
    }
}

class Posting {
    // id of the document
    private int id;
    
    // frequency of the term within this document
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

    @Override
    public boolean equals(Object obj) {
        Posting otherPosting = (Posting) obj;
        if (otherPosting != null) {
            if (this.id == otherPosting.id) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return this.id;
    }
}

class Term {
    // string representation of the term
    private String termStr;
    
    // the length of the postings list for this term
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
    private int numOfComparisons;
    private long runTime; // in seconds
    private List<Integer> docIds;
    private int numCompWtOptimzn;

    public QueryResult() {
        this.numOfComparisons = 0;
        this.runTime = 0L;
        this.numCompWtOptimzn = -1;
        // do not initialize docIds. This is to differentiate between
        // 0 docs and terms not found
    }

    public QueryResult(int numOfComparisons, long runTime, List<Integer> docIds, int numCompWtOptimz) {
        this.numOfComparisons = numOfComparisons;
        this.runTime = runTime;
        this.docIds = docIds;
        this.numCompWtOptimzn = numCompWtOptimz;
    }

    public int getNumCompWtOptimzn() {
        return numCompWtOptimzn;
    }

    public void setNumCompWtOptimzn(int numCompWtOptimzn) {
        this.numCompWtOptimzn = numCompWtOptimzn;
    }

    public int getNumOfComparisons() {
        return numOfComparisons;
    }

    public void setNumOfComparisons(int numOfComparisons) {
        this.numOfComparisons = numOfComparisons;
    }

    public long getRunTime() {
        return runTime;
    }

    public void setRunTime(long l) {
        this.runTime = l;
    }

    public List<Integer> getDocIds() {
        return docIds;
    }

    public void setDocIds(List<Integer> docIds) {
        this.docIds = docIds;
    }
}

interface Index {
    public List<Term> getTopK(int k);

    public List<Posting> getPostings(Term term);

    public QueryResult conjuctiveQuery(List<Term> queryTerms, boolean isOptimization);

    public QueryResult disjunctiveQuery(List<Term> queryTerms, boolean isOptimization);

}

/**
 * index for DAAT strategy
 * 
 * @author kishore
 *
 */
class DocIdOrderedIndex implements Index {

    // the dictionary
    private Map<Term, List<Posting>> idx;

    public DocIdOrderedIndex(String indexFileName) {
        idx = new HashMap<Term, List<Posting>>();

        parseIndexFile(indexFileName);
    }

    /**
     * Method to read the idx file into memory
     * 
     * @param indexFileName
     */
    private void parseIndexFile(String indexFileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(indexFileName))) {
            String line;
            
            // read the file line by line
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

                    int docId = Integer.parseInt(vals[0]);
                    int tf = Integer.parseInt(vals[1]);
                    Posting p = new Posting(docId, tf);

                    // add according to increasing order of docIds
                    if (pList.isEmpty()) {
                        pList.add(p);
                    } else {
                        int i = 0;
                        while (i < pList.size() && pList.get(i).getId() < docId) {
                            ++i;
                        }
                        pList.add(i, p);
                    }
                }

                // put the term and its postings list into the dictionary 
                idx.put(term, pList);
            }
        } catch (FileNotFoundException e) {
            System.err.println("Index file \"" + indexFileName + "\"" + " not found");
        } catch (IOException e) {
            System.err.println("Error while reading the index file");
            e.printStackTrace();
        }
    }

    /** 
     * Method to the get the top 'k' terms according to the non-decreasing length of their postings list
     */
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

    /**
     * DAAT AND
     * @param queryTerms
     * @return
     */
    @Override
    public QueryResult conjuctiveQuery(List<Term> queryTerms, boolean isOptimization) {
        Long startTime = System.currentTimeMillis();
        QueryResult qr = new QueryResult();
        boolean termNotFound = false;
        
        // get the postings lists related to the query terms
        List<List<Posting>> allPostings = new ArrayList<List<Posting>>(queryTerms.size());
        for (Term t : queryTerms) {
            List<Posting> postingForT = idx.get(t);
            if (postingForT != null) {
                allPostings.add(postingForT);
            } else {
                // one of the terms was not found. So return an empty result.
                termNotFound = true;
                break;
            }
        }

        // since we are performing a conjuctive query, if one of the terms was not found then, we say 'terms not found'
        if (!termNotFound) {

            // sort the postings list in increasing order of their sizes
            Collections.sort(allPostings, new Comparator<List<Posting>>() {

                @Override
                public int compare(List<Posting> l1, List<Posting> l2) {
                    int len1 = l1.size();
                    int len2 = l2.size();
                    if (len1 < len2) {
                        return -1;
                    } else if (len1 > len2) {
                        return 1;
                    }
                    return 0;
                }

            });

            int numOfComparisons = 0;
            List<Posting> result = new LinkedList<Posting>();
            
            int[] indices = new int[allPostings.size()];
            boolean empty = false; // set when we reach the end of any postings list
            while(!empty) {
                int equalCount = 0;
                for (int i = 0; i < indices.length - 1; ++i) {
                    if (indices[i] < allPostings.get(i).size() && indices[i + 1] < allPostings.get(i + 1).size()) {
                        Posting p1 = allPostings.get(i).get(indices[i]);
                        Posting p2 = allPostings.get(i + 1).get(indices[i + 1]);
                        ++numOfComparisons;
                        if (p1.getId() == p2.getId()) {
                            ++equalCount;
                        } else if (p1.getId() < p2.getId()) {
                            indices[i] += 1;
                        } else {
                            indices[i+1] += 1;
                        }
                    } else {
                        empty = true;
                    }
                }
                
                if (equalCount == indices.length - 1) {
                    // the doc should have been found in all the postings list
                    result.add(allPostings.get(0).get(indices[0]));
                    for (int i = 0; i < indices.length; ++i) {
                        indices[i] += 1;
                    }
                }
            }

            qr.setNumOfComparisons(numOfComparisons);

            List<Integer> docIds = new ArrayList<Integer>(result.size());
            for (Posting p : result) {
                docIds.add(p.getId());
            }
            qr.setDocIds(docIds);
        }

        // measure the time spent on this operation
        Long endTime = System.currentTimeMillis();
        qr.setRunTime((endTime - startTime) / 1000L);

        return qr;
    }

    /**
     * DAAT OR
     * @param queryTerms
     * @param isOptimization
     * @return
     */
    @Override
    public QueryResult disjunctiveQuery(List<Term> queryTerms, boolean isOptimization) {
        Long startTime = System.currentTimeMillis();
        QueryResult qr = new QueryResult();

        // get the postings lists for the query terms
        List<List<Posting>> allPostings = new ArrayList<List<Posting>>(queryTerms.size());
        for (Term t : queryTerms) {
            List<Posting> postingForT = idx.get(t);
            if (postingForT != null) {
                allPostings.add(postingForT);
            }
        }

        // if there is at least one non-empty postings list start the merge process
        if (allPostings.size() > 0) {        
                       
            int numOfComparisons = 0;
            List<Posting> result = new LinkedList<Posting>();
            
            int[] indices = new int[allPostings.size()];
            int end = 0; // this will be set to number of postings list, when all postings list have been traversed 
            while (end < allPostings.size()) {
                
                // The alignment operation - 
                // Find the docId for the current iteration, that will be the least docId among all
                // the docId's at the beginning of every postings list
                Posting minPosting = new Posting(Integer.MAX_VALUE, 0); // initialize min to some large value
                for (int i = 0; i < indices.length; ++i) {
                    if (indices[i] < allPostings.get(i).size()) {
                        Posting p = allPostings.get(i).get(indices[i]);
                        ++numOfComparisons;
                        if (p.getId() < minPosting.getId()) {
                            minPosting = p;
                        }
                    }
                }
                
                // traverse all the lists concurrently
                for (int i = 0; i < indices.length; ++i) {
                    if (indices[i] < allPostings.get(i).size()) {
                        Posting p2 = allPostings.get(i).get(indices[i]);
                        ++numOfComparisons;
                        while(p2.getId() <= minPosting.getId()){
                            ++numOfComparisons;
                            indices[i] += 1;
                            if (indices[i] >= allPostings.get(i).size()) {
                                ++end;
                                break;
                            }
                            p2 = allPostings.get(i).get(indices[i]);
                        }
                    }
                }
                
                // we have incremented the relevant pointers for each postings list
                // now add the current docId to the result.
                result.add(minPosting);
            }

            List<Integer> docIds = new ArrayList<Integer>(result.size());
            for (Posting p : result) {
                docIds.add(p.getId());
            }
            qr.setDocIds(docIds);

            qr.setNumOfComparisons(numOfComparisons);
        }
        
        // measure the time spent on this operation
        Long endTime = System.currentTimeMillis();
        qr.setRunTime((endTime - startTime) / 1000L);
        return qr;
    }

}
/**
 * index for TAAT strategy
 * 
 * @author kishore
 *
 */
class TermFreqOrderedIndex implements Index {

    // the dictionary
    private Map<Term, List<Posting>> idx;

    public TermFreqOrderedIndex(String indexFileName) {
        idx = new HashMap<Term, List<Posting>>();
        parseIndexFile(indexFileName);
    }

    /**
     * Method to read the idx file into memory
     * 
     * TODO: put the parsing logic into a single method. Abstract Index class? The only change is order according to the strategy
     * 
     * @param indexFileName
     * 
     */
    private void parseIndexFile(String indexFileName) {
        try (BufferedReader br = new BufferedReader(new FileReader(indexFileName))) {
            String line;
            // read the idx file line by line
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

                    // add according to decreasing order of term frequencies
                    if (pList.isEmpty()) {
                        pList.add(p);
                    } else {
                        int i = 0;
                        while (i < pList.size() && pList.get(i).getTermFreq() >= tf) {
                            ++i;
                        }
                        pList.add(i, p);
                    }
                }
                
                // put the term and its postings list into the dictionary
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

    /**
     * TAAT AND
     * 
     * @param queryTerms
     * @param isOptimization
     * @return
     */
    @Override
    public QueryResult conjuctiveQuery(List<Term> queryTerms, boolean isOptimization) {

        Long startTime = System.currentTimeMillis();

        QueryResult qr = new QueryResult();

        // get the postings lists related to the query terms
        boolean termNotFound = false;
        List<List<Posting>> allPostings = new ArrayList<List<Posting>>(queryTerms.size());
        for (Term t : queryTerms) {
            List<Posting> postingForT = idx.get(t);
            if (postingForT != null) {
                allPostings.add(postingForT);
            } else {
                // one of the terms was not found. So return an empty result.
                termNotFound = true;
                break;
            }
        }

        if (!termNotFound) {

            if (isOptimization) {
                // sort the postings list in increasing order of their sizes
                Collections.sort(allPostings, new Comparator<List<Posting>>() {

                    @Override
                    public int compare(List<Posting> l1, List<Posting> l2) {
                        int len1 = l1.size();
                        int len2 = l2.size();
                        if (len1 < len2) {
                            return -1;
                        } else if (len1 > len2) {
                            return 1;
                        }
                        return 0;
                    }

                });
            }

            int numOfComparisons = 0;
            List<Posting> result = new LinkedList<Posting>();
            result.addAll(allPostings.get(0)); // assign result to first
                                               // posting list
            for (int i = 1; i < allPostings.size(); ++i) {
                Iterator<Posting> resultIter = result.iterator(); // iterate on
                                                                  // the
                                                                  // result
                List<Posting> p2 = allPostings.get(i); // next postings list
                while (resultIter.hasNext()) {
                    // check if the current docId is present in p2
                    Posting po = resultIter.next();
                    int j;
                    for (j = 0; j < p2.size(); ++j) {
                        ++numOfComparisons;
                        if (p2.get(j).equals(po)) {
                            break;
                        }
                    }
                    if (j == p2.size()) {
                        // we did not find this doc
                        resultIter.remove();
                    }
                }
            }

            List<Integer> docIds = new ArrayList<Integer>(result.size());
            for (Posting p : result) {
                docIds.add(p.getId());
            }
            qr.setDocIds(docIds);

            qr.setNumOfComparisons(numOfComparisons);
            if (isOptimization) {
                qr.setNumCompWtOptimzn(numOfComparisons);
            }
        }
        // measure the time spent on this operation
        Long endTime = System.currentTimeMillis();
        qr.setRunTime((endTime - startTime) / 1000L);

        return qr;
    }

    /**
     * TAAT OR
     * @param queryTerms
     * @param isOptimization
     * @return
     */
    @Override
    public QueryResult disjunctiveQuery(List<Term> queryTerms, boolean isOptimization) {

        Long startTime = System.currentTimeMillis();

        QueryResult qr = new QueryResult();

        // get the postings lists related to the query term
        List<List<Posting>> allPostings = new ArrayList<List<Posting>>(queryTerms.size());
        for (Term t : queryTerms) {
            List<Posting> postingForT = idx.get(t);
            if (postingForT != null) {
                allPostings.add(postingForT);
            }
        }

        if (allPostings.size() > 0) {
            
            if (isOptimization) {
                // sort the postings list in decreasing order of their sizes
                Collections.sort(allPostings, new Comparator<List<Posting>>() {

                    @Override
                    public int compare(List<Posting> l1, List<Posting> l2) {
                        int len1 = l1.size();
                        int len2 = l2.size();
                        if (len1 < len2) {
                            return 1;
                        } else if (len1 > len2) {
                            return -1;
                        }
                        return 0;
                    }

                });
            }
                                    
            int numOfComparisons = 0;
            List<Posting> result = new LinkedList<Posting>();
            result.addAll(allPostings.get(0)); // assign result to first posting
                                              // list
            for (int i = 1; i < allPostings.size(); ++i) {
                List<Posting> postingsList = allPostings.get(i);
                // compare each docId in the current postings list to the docId's in the intermediate result 
                for (Posting p : postingsList) {
                    boolean found = false;
                    for (Posting resultPosting : result) {
                        ++numOfComparisons;
                        if (resultPosting.equals(p)) {
                            found = true;
                            break;
                        }
                    }
                    if (!found) {
                        // the current docId is not present in the intermediate result
                        // so add it to the intermediate result
                        result.add(p);
                    }
                }
            }

            List<Integer> docIds = new ArrayList<Integer>(result.size());
            for (Posting p : result) {
                docIds.add(p.getId());
            }
            qr.setDocIds(docIds);

            qr.setNumOfComparisons(numOfComparisons);
            if (isOptimization) {
                qr.setNumCompWtOptimzn(numOfComparisons);
            }

        }

        // measure the time spent on this operation
        Long endTime = System.currentTimeMillis();
        qr.setRunTime((endTime - startTime) / 1000L);

        return qr;
    }

}