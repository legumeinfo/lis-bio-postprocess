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
import java.util.HashSet;
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
 * GeneFamilyTally.totalCount
 * GeneFamilyTally.numAnnotations
 * GeneFamilyTally.averageCount
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
        gftList.clear();
        System.out.println("## Deleted GeneFamilyTally objects.");
        LOG.info("Deleted GeneFamilyTally objects.");
        // now query GeneFamily and tally gene counts per organism
        List<GeneFamily> gfList = new ArrayList<>();
        //
        Query qGeneFamily = new Query();
        QueryClass qcGeneFamily = new QueryClass(GeneFamily.class);
        qGeneFamily.addFrom(qcGeneFamily);
        qGeneFamily.addToSelect(qcGeneFamily);
        // execute the query
        Results results = osw.getObjectStore().execute(qGeneFamily);
	for (Object resultObject : results.asList()) {
            Map<String,Organism> organismMap = new HashMap<>();      // keyed by taxonId
            Map<String,Integer> totalCountMap = new HashMap<>();     // keyed by taxonId
            Map<String,Integer> numAnnotationsMap = new HashMap<>(); // keyed by taxonId
            Set<String> annotations = new HashSet<>();               // store the annotations that we've seen: taxonId.strainIdentifier.assemblyVersion.annotationVersion
	    ResultsRow row = (ResultsRow) resultObject;
            // --
            // use clone so we can store updated object below
            GeneFamily gf = PostProcessUtil.cloneInterMineObject((GeneFamily) row.get(0));
            gfList.add(gf);
            // --
            Set<Gene> genes = gf.getGenes();
            for (Gene g : genes) {
                Organism organism = g.getOrganism();
                String taxonId = organism.getTaxonId();
                String strainIdentifier = g.getStrain().getIdentifier();
                String assemblyVersion = g.getAssemblyVersion();
                String annotationVersion = g.getAnnotationVersion();
                String annotation = taxonId + "." + strainIdentifier + "." + assemblyVersion + "." + annotationVersion;
                if (organismMap.containsKey(taxonId)) {
                    // increment this organism's totalCount
                    totalCountMap.put(taxonId, totalCountMap.get(taxonId) + 1);
                } else {
                    // new organism, initialize totalCount = 1
                    organismMap.put(taxonId, organism);
                    totalCountMap.put(taxonId, 1);
                }
                if (!annotations.contains(annotation)) {
                    annotations.add(annotation);
                    numAnnotationsMap.put(taxonId, annotations.size());
                }
            }
            // sum the tallies for GeneFamily.size
            int size = 0;
            for (int totalCount : totalCountMap.values()) {
                size += totalCount;
            }
            gf.setFieldValue("size", size);
            // add the GeneFamilyTally objects for each organism
            for (String taxonId : totalCountMap.keySet()) {
                Organism organism = organismMap.get(taxonId);
                int totalCount = totalCountMap.get(taxonId);
                int numAnnotations = numAnnotationsMap.get(taxonId);
                double averageCount = (double) totalCount / (double) numAnnotations;
                GeneFamilyTally gft = (GeneFamilyTally) DynamicUtil.createObject(Collections.singleton(GeneFamilyTally.class));
                gftList.add(gft);
                gft.setGeneFamily(gf);
                gft.setOrganism(organism);
                gft.setTotalCount(totalCount);
                gft.setNumAnnotations(numAnnotations);
                gft.setAverageCount(averageCount);
            }
        }
        // store the GeneFamily objects
        osw.beginTransaction();
        for (GeneFamily gf : gfList) {
            osw.store(gf);
        }
        osw.commitTransaction();
        // store the GeneFamilyTally objects
        osw.beginTransaction();
        for (GeneFamilyTally gft : gftList) {
            osw.store(gft);
        }
        osw.commitTransaction();
    }
    
}
