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
import java.util.concurrent.ConcurrentHashMap;

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
     * @param osw object store writer
     */
    public PopulatePanGeneSetGenesProteinsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        // query PanGeneSets
        Map<Integer,PanGeneSet> panGeneSets = new HashMap<>();
        Query qPanGeneSet = new Query();
        QueryClass qcPanGeneSet = new QueryClass(PanGeneSet.class);
        qPanGeneSet.addFrom(qcPanGeneSet);
        qPanGeneSet.addToSelect(qcPanGeneSet);
        Results results = osw.getObjectStore().execute(qPanGeneSet);
        for (Object obj : results.asList()) {
            ResultsRow row = (ResultsRow) obj;
            PanGeneSet panGeneSet = (PanGeneSet) row.get(0);
            if (panGeneSet.getGenes().size() == 0 || panGeneSet.getProteins().size() == 0) {
                // only update pan-gene sets that haven't already been updated
                panGeneSets.put(panGeneSet.getId(), panGeneSet);
            }
        }
        LOG.info("Found " + panGeneSets.size() + " PanGeneSets without genes and/or proteins.");

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        // spin through the PanGeneSets, getting their transcripot, and that transcript's, protein and that protein's genes.
        Map<Integer, Set<Protein>> panGeneSetProteins = new ConcurrentHashMap<>();
        Map<Integer, Set<Gene>> panGeneSetGenes = new ConcurrentHashMap<>();
        panGeneSets.keySet().parallelStream().forEach(id -> {
                PanGeneSet panGeneSet = panGeneSets.get(id);
                Set<Protein> proteins = new HashSet<>();
                Set<Gene> genes = new HashSet<>();
                for (Transcript transcript : panGeneSet.getTranscripts()) {
                    if (transcript.getProtein() != null && transcript.getProtein().getGenes() != null) {
                        proteins.add(transcript.getProtein());
                        genes.addAll(transcript.getProtein().getGenes());
                    }
                }
                panGeneSetProteins.put(id, proteins);
                panGeneSetGenes.put(id, genes);
            });
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // now store our protein and gene sets with the PanGeneSets
        LOG.info("Adding proteins and genes to " + panGeneSetProteins.size() + " PanGeneSets.");
        osw.beginTransaction();
        for (int id : panGeneSetProteins.keySet()) {
            PanGeneSet panGeneSet = panGeneSets.get(id);
            try {
                PanGeneSet clone = PostProcessUtil.cloneInterMineObject(panGeneSet);
                clone.setProteins(panGeneSetProteins.get(id));
                clone.setGenes(panGeneSetGenes.get(id));
                osw.store(clone);
            } catch (IllegalAccessException ex) {
                System.err.println(ex);
                throw new RuntimeException(ex);
            }
        }
        osw.commitTransaction();
    }
}
