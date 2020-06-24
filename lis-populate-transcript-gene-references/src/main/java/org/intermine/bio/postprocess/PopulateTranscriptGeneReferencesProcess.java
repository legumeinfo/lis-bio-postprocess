package org.intermine.bio.postprocess;

/*
 * Copyright (C) 2002-2016 FlyMine, Legume Federation
 *
 * This code may be freely distributed and modified under the
 * terms of the GNU Lesser General Public Licence.  This should
 * be distributed with the code.  See the LICENSE file for more
 * information or http://www.gnu.org/copyleft/lesser.html.
 *
 */

import java.util.List;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.Transcript;
import org.intermine.model.bio.Gene;

import org.apache.log4j.Logger;

/**
 * Fill the Transcript.gene references by matching gene primaryIdentifier.1 with Transcript.primaryIdentifier.
 *
 * @author Sam Hokin
 */
public class PopulateTranscriptGeneReferencesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateTranscriptGeneReferencesProcess.class);

    /**
     * Populate a new instance of PopulateTranscriptGeneReferencesProcess
     * @param osw object store writer
-     */
    public PopulateTranscriptGeneReferencesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
	// 0
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        q.addFrom(qcTranscript);
        q.addToSelect(qcTranscript);
	// 1
        QueryClass qcGene = new QueryClass(Gene.class);
        q.addFrom(qcGene);
        q.addToSelect(qcGene);
	// transcript.primaryIdentifier = gene.primaryIdentifier.1 (primary Transcript)
	QueryField transcriptPrimaryIdentifier = new QueryField(qcTranscript, "primaryIdentifier");
	QueryField genePrimaryIdentifier = new QueryField(qcGene, "primaryIdentifier");
	QueryValue dot1 = new QueryValue(".1");
	QueryExpression genePrimaryIdentifierMatch = new QueryExpression(genePrimaryIdentifier, QueryExpression.CONCAT, dot1);
	SimpleConstraint sc = new SimpleConstraint(transcriptPrimaryIdentifier, ConstraintOp.EQUALS, genePrimaryIdentifierMatch);
	q.setConstraint(sc);
	
        // execute the query
        Results results = osw.getObjectStore().execute(q);
	List<Object> resultObjects = results.asList();
	
        // begin transaction for storing and run through the matches
	osw.beginTransaction();
	for (Object resultObject : resultObjects) {
	    ResultsRow row = (ResultsRow) resultObject;
	    try {
		Transcript transcript = PostProcessUtil.cloneInterMineObject((Transcript) row.get(0));
		Gene gene = (Gene) row.get(1);
		transcript.setFieldValue("gene", gene);
		osw.store(transcript);
	    } catch (IllegalAccessException e) {
		throw new ObjectStoreException(e);
	    }
	}
	osw.commitTransaction();
        osw.close();
    }
        
}
