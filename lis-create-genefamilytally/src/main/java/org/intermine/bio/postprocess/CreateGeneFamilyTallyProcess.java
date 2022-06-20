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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;

import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneFamily;
import org.intermine.model.bio.GeneFamilyTally;
import org.intermine.model.bio.Organism;

import org.intermine.util.DynamicUtil;

import org.apache.log4j.Logger;

/**
 * Create GeneFamilyTally objects which store the number of genes per gene family per organism.
 * Also populates GeneFamily.size for number of genes per gene family.
 *
 * GeneFamily.size
 *
 * GeneFamilyTally.tally
 * GeneFamilyTally.organism
 * GeneFamilyTally.geneFamily
 *
 * @author Sam Hokin
 */
public class CreateGeneFamilyTallyProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(CreateGeneFamilyTallyProcess.class);

    /**
     * Populate a new instance of PopulateCDSGeneProcess
     * @param osw object store writer
-     */
    public CreateGeneFamilyTallyProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException, IllegalAccessException {
        // clear out the existing GeneFamilyTally objects
        // we have to do them one by one because delete(QueryClass) seems to be broken
        Query qGeneFamilyTally = new Query();
        QueryClass qcGeneFamilyTally = new QueryClass(GeneFamilyTally.class);
        qGeneFamilyTally.addFrom(qcGeneFamilyTally);
        qGeneFamilyTally.addToSelect(qcGeneFamilyTally);
        List<GeneFamilyTally> gftList = new ArrayList<>();
        Results gftResults = osw.getObjectStore().execute(qGeneFamilyTally);
        for (Object o : gftResults.asList()) {
            ResultsRow row = (ResultsRow) o;
            GeneFamilyTally gft = (GeneFamilyTally) row.get(0);
            gftList.add(gft);
        }
        for (GeneFamilyTally gft : gftList) {
            osw.beginTransaction();
            osw.delete(gft);
            osw.commitTransaction();
        }
        LOG.info("Deleted GeneFamilyTally objects.");
        // now query GeneFamily and tally gene counts per organism
        Query qGeneFamily = new Query();
        QueryClass qcGeneFamily = new QueryClass(GeneFamily.class);
        qGeneFamily.addFrom(qcGeneFamily);
        qGeneFamily.addToSelect(qcGeneFamily);
        qGeneFamily.addToOrderBy(new QueryField(qcGeneFamily, "identifier"), "desc"); 
        // execute the query
        Results results = osw.getObjectStore().execute(qGeneFamily);
	for (Object resultObject : results.asList()) {
            Map<String,Integer> tallyMap = new HashMap<>();      // keyed by taxonId
            Map<String,Organism> organismMap = new HashMap<>();  // keyed by taxonId
	    ResultsRow row = (ResultsRow) resultObject;
            // --
            GeneFamily gf = (GeneFamily) row.get(0);             // 0
            // --
            Set<Gene> genes = gf.getGenes();
            for (Gene g : genes) {
                Organism organism = g.getOrganism();
                String taxonId = organism.getTaxonId();
                if (organismMap.containsKey(taxonId)) {
                    // increment this organism's tally
                    tallyMap.put(taxonId, tallyMap.get(taxonId) + 1);
                } else {
                    // new organism, initialize tally = 1
                    organismMap.put(taxonId, organism);
                    tallyMap.put(taxonId, 1);
                }
            }
            // sum the tallies for GeneFamily.size
            int size = 0;
            for (int tally : tallyMap.values()) {
                size += tally;
            }
            gf.setFieldValue("size", gf.getGenes().size());
            // create and store GeneFamilyTally objects for each organism
            osw.beginTransaction();
            for (String taxonId : tallyMap.keySet()) {
                GeneFamilyTally gft = (GeneFamilyTally) DynamicUtil.createObject(Collections.singleton(GeneFamilyTally.class)); 
                gft.setGeneFamily(gf);
                gft.setOrganism(organismMap.get(taxonId));
                gft.setTally(tallyMap.get(taxonId));
                // DEBUG
                System.out.println("## STORING:"+gft.getId()+":"+gft.getGeneFamily().getIdentifier()+":"+gft.getOrganism().getTaxonId()+":"+gft.getTally());
                //
                osw.store(gft);
            }
            osw.commitTransaction();
	}
    }
    
}
