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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.MultipleInBagConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;

import org.intermine.model.bio.Gene;
import org.intermine.model.bio.PanGeneSet;
import org.intermine.model.bio.Protein;
import org.intermine.model.bio.Transcript;

import org.apache.log4j.Logger;

/**
 * Populate PanGeneSet.genes and PanGeneSet.proteins using already-loaded PanGeneSet.transcripts.
 *
 * @author Sam Hokin
 */
public class PopulatePanGeneSetGenesProteinsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulatePanGeneSetGenesProteinsProcess.class);

    /**
     * Populate a new instance of PopulateQTLMarkersProcess
     *
     * @param osw object store writer
     */
    public PopulatePanGeneSetGenesProteinsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        // query Transcripts that have gene and protein references
        List<Transcript> transcripts = new ArrayList<>();
        Query qTranscript = new Query();
        QueryClass qcTranscript = new QueryClass(Transcript.class);
        qTranscript.addFrom(qcTranscript);
        qTranscript.addToSelect(qcTranscript);
        Results results = osw.getObjectStore().execute(qTranscript);
        for (Object obj : results.asList()) {
            ResultsRow row = (ResultsRow) obj;
            Transcript transcript = (Transcript) row.get(0);
            if (transcript.getProtein() != null) {
                transcripts.add(transcript);
            }
        }
        LOG.info("Found " + transcripts.size() + " transcripts with protein references.");

        // spin through the Transcripts, getting their panGeneSets, protein and the protein's genes
        // then add the protein and genes to the panGeneSet maps.
        // Note: these sets are ProxyCollections and cannot be used in typical Set operations!
        Map<PanGeneSet, Set<Protein>> panGeneSetProteins = new HashMap<>();
        Map<PanGeneSet, Set<Gene>> panGeneSetGenes = new HashMap<>();
        for (Transcript transcript : transcripts) {
            final Protein protein = transcript.getProtein();
            final Set<Gene> genes = protein.getGenes();
            for (PanGeneSet panGeneSet : transcript.getPanGeneSets()) {
                if (panGeneSetProteins.containsKey(panGeneSet)) {
                    panGeneSetProteins.get(panGeneSet).add(protein);
                } else {
                    // create a proper HashSet
                    Set<Protein> set = new HashSet<>();
                    set.add(protein);
                    panGeneSetProteins.put(panGeneSet, set);
                }
                if (panGeneSetGenes.containsKey(panGeneSet)) {
                    for (Gene gene : genes) {
                        panGeneSetGenes.get(panGeneSet).add(gene);
                    }
                } else {
                    // create a proper HashSet
                    Set<Gene> set = new HashSet<>();
                    set.addAll(genes);
                    panGeneSetGenes.put(panGeneSet, set);
                }
            }
        }

        // now store our sets
        osw.beginTransaction();
        for (PanGeneSet pgs : panGeneSetProteins.keySet()) {
            try {
                PanGeneSet pgsClone = PostProcessUtil.cloneInterMineObject(pgs);
                pgsClone.setProteins(panGeneSetProteins.get(pgs));
                pgsClone.setGenes(panGeneSetGenes.get(pgs));
                osw.store(pgsClone);
            } catch (IllegalAccessException ex) {
                System.err.println(ex);
                throw new RuntimeException(ex);
            }
        }
        osw.commitTransaction();
    }
}
