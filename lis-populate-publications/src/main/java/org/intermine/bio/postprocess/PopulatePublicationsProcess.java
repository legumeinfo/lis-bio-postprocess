package org.intermine.bio.postprocess;
/*
 * Copyright (C) 2002-2017 FlyMine, Legume Federation
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import javax.xml.parsers.ParserConfigurationException;
import org.xml.sax.SAXException;

import org.intermine.model.bio.Author;
import org.intermine.model.bio.Publication;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.OrderDescending;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.ParseException;

import org.ncgr.crossref.WorksQuery;
import org.ncgr.pubmed.PubMedSummary;

/**
 * Populate data and authors for publications from CrossRef and PubMed.
 *
 * @author Sam Hokin
 */
public class PopulatePublicationsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulatePublicationsProcess.class);
    private static final String API_KEY = "48cb39fb23bf1190394ccbae4e1d35c5c809";

    // hold authors in a map so we don't store dupes; keyed by name
    Map<String,Author> authorMap = new HashMap<String,Author>();

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulatePublicationsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException, IllegalAccessException, IOException, ParserConfigurationException, SAXException, ParseException {
        // delete existing Author objects, first loading them into a collection
        // this is necessary because otherwise we get duplicate Author objects
        // this is also why we query ALL publications, not just those that need to be populated
        Query qAuthor = new Query();
        qAuthor.setDistinct(true);
        ConstraintSet csAuthor = new ConstraintSet(ConstraintOp.AND);
        // 0 Author
        QueryClass qcAuthor = new QueryClass(Author.class);
        qAuthor.addToSelect(qcAuthor);
        qAuthor.addFrom(qcAuthor);
        Set<Author> authorSet = new HashSet<Author>();
        Results authorResults = osw.getObjectStore().execute(qAuthor);
        Iterator<?> authorIter = authorResults.iterator();
        while (authorIter.hasNext()) {
            ResultsRow<?> rr = (ResultsRow<?>) authorIter.next();
            authorSet.add((Author)rr.get(0));
        }
        // delete them one by one (because bulk deletion is broken)
        // NOTE: this does not delete authorspublications records. That is a bug IMO.
        for (Author author : authorSet) {
            osw.beginTransaction();
            osw.delete(author);
            osw.commitTransaction();
        }
        
        // query all publications
        Query qPub = new Query();
        qPub.setDistinct(true);
        QueryClass qcPub = new QueryClass(Publication.class);
        qPub.addToSelect(qcPub);
        qPub.addFrom(qcPub);
        
        // execute the query
        Results pubResults = osw.getObjectStore().execute(qPub);
        Iterator<?> pubIter = pubResults.iterator();
        while (pubIter.hasNext()) {
            ResultsRow<?> rrPub = (ResultsRow<?>) pubIter.next();
            Publication pub = (Publication) rrPub.get(0);
            // field values
            String title = stringOrNull(pub.getFieldValue("title"));
            String firstAuthor = stringOrNull(pub.getFieldValue("firstAuthor"));
            // core IM model does not contain lastAuthor
            // String lastAuthor = stringOrNull(pub.getFieldValue("lastAuthor"));
            String month = stringOrNull(pub.getFieldValue("month"));
            int year = intOrZero(pub.getFieldValue("year"));
            String journal = stringOrNull(pub.getFieldValue("journal"));
            String volume = stringOrNull(pub.getFieldValue("volume"));
            String issue = stringOrNull(pub.getFieldValue("issue"));
            String pages = stringOrNull(pub.getFieldValue("pages"));
            String abstractText = stringOrNull(pub.getFieldValue("abstractText"));
            int pubMedId = intOrZero(pub.getFieldValue("pubMedId"));
            String doi = stringOrNull(pub.getFieldValue("doi"));
            LOG.info("##### "+doi);
            if (doi==null) {
                LOG.error("Publication missing DOI: "+title);
                continue;
            }

            // query PubMed for data from DOI to get PMID
            if (pubMedId==0) {
                PubMedSummary summary = new PubMedSummary();
                summary.searchDOI(doi, API_KEY);
                if (summary.id!=0) {
                    pubMedId = summary.id;
                    LOG.info("PMID found from DOI:"+pubMedId);
                }
            }

            // query CrossRef entry from DOI
            WorksQuery wq = null;
            JSONArray authors = null;
            wq = new WorksQuery(doi);
            if (wq.getStatus()!=null && wq.getStatus().equals("ok")) {
                // update attributes from CrossRef
                title = wq.getTitle();
                try { year = wq.getJournalIssueYear(); } catch (Exception ex) { }
                if (year==0) {
                    try { year = wq.getIssuedYear(); } catch (Exception ex) { }
                }
                try { month = String.valueOf(wq.getJournalIssueMonth()); } catch (Exception ex) { }
                if (month==null) {
                    try { month = String.valueOf(wq.getIssuedMonth()); } catch (Exception ex) { }
                }
                if (wq.getShortContainerTitle()!=null) {
                    journal = wq.getShortContainerTitle();
                } else if (wq.getContainerTitle()!=null) {
                    journal = wq.getContainerTitle();
                }
                volume = wq.getVolume();
                issue = wq.getIssue();
                pages = wq.getPage();
                doi = wq.getDOI();
                authors = wq.getAuthors();
                if (authors!=null && authors.size()>0) {
                    JSONObject firstAuthorObject = (JSONObject) authors.get(0);
                    firstAuthor = (String) firstAuthorObject.get("family");
                    if (firstAuthorObject.get("given")!=null) firstAuthor += ", " + (String) firstAuthorObject.get("given");
                }
                
                // core IM model does not contain lastAuthor
                // if (authors.size()>1) {
                //     JSONObject lastAuthorObject = (JSONObject) authors.get(authors.size()-1);
                //     lastAuthor = lastAuthorObject.get("family")+", "+lastAuthorObject.get("given");
                // }

                // update publication object
                Publication tempPub = PostProcessUtil.cloneInterMineObject(pub);
                if (title!=null) tempPub.setFieldValue("title", title);
                if (firstAuthor!=null) tempPub.setFieldValue("firstAuthor", firstAuthor);
                if (month!=null && !month.equals("0")) tempPub.setFieldValue("month", month);
                if (year>0) tempPub.setFieldValue("year", year);
                if (journal!=null) tempPub.setFieldValue("journal", journal);
                if (volume!=null) tempPub.setFieldValue("volume", volume);
                if (issue!=null) tempPub.setFieldValue("issue", issue);
                if (pages!=null) tempPub.setFieldValue("pages", pages);
                if (pubMedId>0) tempPub.setFieldValue("pubMedId", String.valueOf(pubMedId));
                if (doi!=null) tempPub.setFieldValue("doi", doi);
                // core IM model does not contain lastAuthor
                // if (lastAuthor!=null) tempPub.setFieldValue("lastAuthor", lastAuthor);
                
                if (authors!=null) {
                    // update publication.authors from CrossRef since it provides given and family names; given often includes initials, e.g. "Douglas R"
                    // place this pub's authors in a set to add to its authors collection; store the ones that are new
                    authorSet = new HashSet<Author>();
                    for (Object authorObject : authors)  {
                        JSONObject authorJSON = (JSONObject) authorObject;
                        // IM Author attributes from CrossRef fields
                        String firstName = null; // there are rare occasions when firstName is missing, so we'll fill that in with a placeholder "X"
                        if (authorJSON.get("given")==null) {
                            firstName = "X";
                        } else {
                            firstName = (String) authorJSON.get("given");
                        }
                        // we require lastName, so if it's missing then bail on this author
                        if (authorJSON.get("family")==null) continue;
                        String lastName = (String) authorJSON.get("family");
                        // split out initials if present
                        // R. K. => R K
                        // R.K.  => R K
                        // Douglas R => Douglas R
                        // Douglas R. => Douglas R
                        String initials = null;
                        // deal with space
                        String[] parts = firstName.split(" ");
                        if (parts.length==2) {
                            if (parts[1].length()==1) {
                                firstName = parts[0];
                                initials = parts[1];
                            } else if (parts[1].length()==2 && parts[1].endsWith(".")) {
                                firstName = parts[0];
                                initials = parts[1].substring(0,1);
                            }
                        }
                        // pull initial out if it's an R.K. style first name (but not M.V.K.)
                        if (initials==null && firstName.length()==4 && firstName.charAt(1)=='.' && firstName.charAt(3)=='.') {
                            initials = String.valueOf(firstName.charAt(2));
                            firstName = String.valueOf(firstName.charAt(0));
                        }
                        // remove trailing period from a remaining R. style first name
                        if (firstName.length()==2 && firstName.charAt(1)=='.') {
                            firstName = String.valueOf(firstName.charAt(0));
                        }
                        // name is used as key, ignore initials since sometimes there sometimes not
                        String name = firstName+" "+lastName;
                        Author author;
                        if (authorMap.containsKey(name)) {
                            author = authorMap.get(name);
                        } else {
                            author = (Author) DynamicUtil.createObject(Collections.singleton(Author.class));
                            author.setFieldValue("firstName", firstName);
                            author.setFieldValue("lastName", lastName);
                            author.setFieldValue("name", name);
                            if (initials!=null) author.setFieldValue("initials", initials);
                            osw.beginTransaction();
                            osw.store(author);
                            osw.commitTransaction();
                            authorMap.put(name, author);
                        }
                        authorSet.add(author);
                    }
                    // put these authors into the pub authors collection
                    if (authorSet.size()>0) {
                        tempPub.setFieldValue("authors", authorSet);
                    }
                }
                // store this publication
                osw.beginTransaction();
                osw.store(tempPub);
                osw.commitTransaction();
            }
        }
    }

    /**
     * Return the trimmed string value of a field, or null
     */
    String stringOrNull(Object fieldValue) {
        if (fieldValue==null) {
            return null;
        } else {
            return ((String)fieldValue).trim();
        }
    }

    /**
     * Return the int value of a field, or zero if null or not parseable into an int
     */
    int intOrZero(Object fieldValue) {
        if (fieldValue==null) {
            return 0;
        } else {
            try {
                int intValue = (int)(Integer)fieldValue;
                return intValue;
            } catch (Exception e1) {
                // probably a String, not Integer
                String stringValue = (String)fieldValue;
                try {
                    int intValue = Integer.parseInt(stringValue);
                    return intValue;
                } catch (Exception e2) {
                    return 0;
                }
            }
        }
    }

    /**
     * Parse the year from a string like "2017 Mar" or "2017 Feb 3"
     */
    static int getYear(String dateString) {
        String[] parts = dateString.split(" ");
        return Integer.parseInt(parts[0]);
    }

    /**
     * Parse the month from a string like "2017 Mar" or "2017 Feb 3"
     */
    static String getMonth(String dateString) {
        String[] parts = dateString.split(" ");
        if (parts.length>1) {
            return parts[1];
        } else {
            return null;
        }
    }
}
