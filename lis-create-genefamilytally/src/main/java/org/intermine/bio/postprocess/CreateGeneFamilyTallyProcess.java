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
import java.util.concurrent.ConcurrentHashMap;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;

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
        Query qGeneFamilyTally = new Query();
        QueryClass qcGeneFamilyTally = new QueryClass(GeneFamilyTally.class);
        qGeneFamilyTally.addFrom(qcGeneFamilyTally);
        qGeneFamilyTally.addToSelect(qcGeneFamilyTally);
        List<GeneFamilyTally> gfts = new ArrayList<>();
        Results gftResults = osw.getObjectStore().execute(qGeneFamilyTally);
        for (Object o : gftResults.asList()) {
            ResultsRow row = (ResultsRow) o;
            GeneFamilyTally gft = (GeneFamilyTally) row.get(0);
            gfts.add(gft);
        }
        osw.beginTransaction();
        for (GeneFamilyTally gft : gfts) {
            osw.delete(gft);
        }
        osw.commitTransaction();
        LOG.info("Deleted " + gfts.size() + " GeneFamilyTally objects.");

        // now query GeneFamily and store in a List
        List<GeneFamily> geneFamilies = new ArrayList<>();
        Query qGeneFamily = new Query();
        QueryClass qcGeneFamily = new QueryClass(GeneFamily.class);
        qGeneFamily.addFrom(qcGeneFamily);
        qGeneFamily.addToSelect(qcGeneFamily);
        Results results = osw.getObjectStore().execute(qGeneFamily);
	for (Object resultObject : results.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            geneFamilies.add((GeneFamily) row.get(0));
        }
        LOG.info("Got " + geneFamilies.size() + " GeneFamily objects.");

        ///////////////////////////////////////////////////////
        // spin through the gene families, accumulating tallies
        Set<GeneFamilyTally> geneFamilyTallies = new HashSet<GeneFamilyTally>();
        Map<String,Organism> organismMap = new ConcurrentHashMap<>();     // keyed by taxonId
        for (GeneFamily geneFamily : geneFamilies) {
            Set<String> annotations = new HashSet<>();                // taxonId.strainIdentifier.assemblyVersion.annotationVersion
            Map<String,Integer> numAnnotationsMap = new HashMap<>();  // keyed by taxonId
            Map<String,Integer> totalCountMap = new HashMap<>();      // keyed by taxonId
            for (Gene gene : geneFamily.getGenes()) {
                // check that this isn't an orphan gene (e.g. from a GFA load only, no annotation)
                if (gene.getOrganism() == null || gene.getStrain() == null || gene.getAssemblyVersion() == null || gene.getAnnotationVersion() == null) {
                    LOG.warn("Gene " + gene.getId() + " lacks required attributes and will be ignored in tally.");
                } else {
                    // get our organism and create our custom annotation key
                    Organism organism = gene.getOrganism();
                    String taxonId = organism.getTaxonId();
                    String strainIdentifier = gene.getStrain().getIdentifier();
                    String assemblyVersion = gene.getAssemblyVersion();
                    String annotationVersion = gene.getAnnotationVersion();
                    String annotation = taxonId + "." + strainIdentifier + "." + assemblyVersion + "." + annotationVersion;
                    // increment the number of annotations for this organism
                    if (!annotations.contains(annotation)) {
                        annotations.add(annotation);
                        if (numAnnotationsMap.containsKey(taxonId)) {
                            numAnnotationsMap.put(taxonId, numAnnotationsMap.get(taxonId) + 1);
                        } else {
                            numAnnotationsMap.put(taxonId, 1);
                        }
                    }
                    // increment the total counts for this organism
                    if (totalCountMap.containsKey(taxonId)) {
                        totalCountMap.put(taxonId, totalCountMap.get(taxonId) + 1);
                    } else {
                        totalCountMap.put(taxonId, 1);
                    }
                    // new organism? add to organismMap for later storage
                    if (!organismMap.containsKey(taxonId)) {
                        organismMap.put(taxonId, organism);
                    }
                }
            }
            // sum the gene tallies and set GeneFamily.size
            int size = 0;
            for (int totalCount : totalCountMap.values()) {
                size += totalCount;
            }
            geneFamily.setSize(size);
            // add the GeneFamilyTally objects for each organism
            for (String taxonId : totalCountMap.keySet()) {
                Organism organism = organismMap.get(taxonId);
                int totalCount = totalCountMap.get(taxonId);
                int numAnnotations = numAnnotationsMap.get(taxonId);
                double averageCount = (double) totalCount / (double) numAnnotations;
                GeneFamilyTally gft = (GeneFamilyTally) DynamicUtil.createObject(Collections.singleton(GeneFamilyTally.class));
                gft.setGeneFamily(geneFamily);
                gft.setOrganism(organism);
                gft.setTotalCount(totalCount);
                gft.setNumAnnotations(numAnnotations);
                gft.setAverageCount(averageCount);
                geneFamilyTallies.add(gft);
            }
        }

        // store the GeneFamily objects
        LOG.info("Storing " + geneFamilies.size() + " update GeneFamily objects.");
        osw.beginTransaction();
        for (GeneFamily geneFamily : geneFamilies) {
            GeneFamily clone = PostProcessUtil.cloneInterMineObject(geneFamily);
            osw.store(clone);
        }
        osw.commitTransaction();

        // store the GeneFamilyTally objects (no clone needed since deleted earlier)
        LOG.info("Storing " + geneFamilyTallies.size() + " GeneFamilyTally objects.");
        osw.beginTransaction();
        for (GeneFamilyTally gft : geneFamilyTallies) {
            osw.store(gft);
        }
        osw.commitTransaction();
    }
}
