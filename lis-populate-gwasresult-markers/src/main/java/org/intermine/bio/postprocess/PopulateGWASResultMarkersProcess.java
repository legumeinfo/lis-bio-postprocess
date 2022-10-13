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

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;
import org.intermine.objectstore.ObjectStore;
import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.intermine.ObjectStoreInterMineImpl;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryExpression;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.intermine.model.bio.GWAS;
import org.intermine.model.bio.GWASResult;
import org.intermine.model.bio.GeneticMarker;

import org.apache.log4j.Logger;

/**
 * Populate GWASResult.markers collections by matching GeneticMarker.name to markerName.
 *
 * @author Sam Hokin
 */
public class PopulateGWASResultMarkersProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateGWASResultMarkersProcess.class);

    /**
     * Populate a new instance of PopulateGWASResultMarkersProcess
     *
     * @param osw object store writer
     */
    public PopulateGWASResultMarkersProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException {
        Query q = new Query();
	// 0
        QueryClass qcGWASResult = new QueryClass(GWASResult.class);
        q.addFrom(qcGWASResult);
        q.addToSelect(qcGWASResult);
	// 1
        QueryClass qcMarker = new QueryClass(GeneticMarker.class);
        q.addFrom(qcMarker);
        q.addToSelect(qcMarker);
        // 2
        QueryClass qcGWAS = new QueryClass(GWAS.class);
        q.addFrom(qcGWAS);
        // constraints
        ConstraintSet constraints = new ConstraintSet(ConstraintOp.AND);
        // GWASResult.gwas = GWAS
        QueryObjectReference gwasResultGWAS = new QueryObjectReference(qcGWASResult, "gwas");
        constraints.addConstraint(new ContainsConstraint(gwasResultGWAS, ConstraintOp.CONTAINS, qcGWAS));
	// GWASResult.markerName = GeneticMarker.name
	QueryField gwasResultMarkerName = new QueryField(qcGWASResult, "markerName");
	QueryField markerName = new QueryField(qcMarker, "name");
	constraints.addConstraint(new SimpleConstraint(gwasResultMarkerName, ConstraintOp.EQUALS, markerName));
        // GWAS.genotypingPlatform = GeneticMarker.genotypingPlatform
        QueryField gwasGenotypingPlatform = new QueryField(qcGWAS, "genotypingPlatform");
        QueryField markerGenotypingPlatform = new QueryField(qcMarker, "genotypingPlatform");
	constraints.addConstraint(new SimpleConstraint(markerGenotypingPlatform, ConstraintOp.EQUALS, gwasGenotypingPlatform));
        q.setConstraint(constraints);


	    // Query qGene = new Query();
	    // qGene.setDistinct(false);
	    // ConstraintSet csGene = new ConstraintSet(ConstraintOp.AND);
	    // // 0 Gene
	    // QueryClass qcGene = new QueryClass(Gene.class);
	    // qGene.addFrom(qcGene);
	    // qGene.addToSelect(qcGene);
	    // // 1 Gene.chromosome
	    // QueryClass qcGeneChromosome = new QueryClass(Chromosome.class);
	    // qGene.addFrom(qcGeneChromosome);
	    // qGene.addToSelect(qcGeneChromosome);
	    // QueryObjectReference geneChromosome = new QueryObjectReference(qcGene, "chromosome");
	    // csGene.addConstraint(new ContainsConstraint(geneChromosome, ConstraintOp.CONTAINS, qcGeneChromosome));
	    // // 2 Gene.chromosomeLocation
	    // QueryClass qcGeneLocation = new QueryClass(Location.class);
	    // qGene.addFrom(qcGeneLocation);
	    // qGene.addToSelect(qcGeneLocation);
	    // QueryObjectReference geneLocation = new QueryObjectReference(qcGene, "chromosomeLocation");
	    // csGene.addConstraint(new ContainsConstraint(geneLocation, ConstraintOp.CONTAINS, qcGeneLocation));
	    // // require that the gene's chromosome equal that of QTLSpan
	    // QueryValue qtlChromosomeIdValue = new QueryValue(qtlSpan.chromosomeId);
	    // QueryField geneChromosomeIdField = new QueryField(qcGeneChromosome, "primaryIdentifier");
	    // SimpleConstraint sameChromosomeConstraint = new SimpleConstraint(qtlChromosomeIdValue, ConstraintOp.EQUALS, geneChromosomeIdField);
	    // csGene.addConstraint(sameChromosomeConstraint);



        // execute the query
        Results results = osw.getObjectStore().execute(q);
        // store GeneticMarkers in a map keyed by GWASResult.id
        Map<Integer,GWASResult> gwasResults = new HashMap<>();
        Map<Integer,Set<GeneticMarker>> gwasResultMarkers = new HashMap<>();
	for (Object resultObject : results.asList()) {
	    ResultsRow row = (ResultsRow) resultObject;
            GWASResult gwasResult = (GWASResult) row.get(0);
            GeneticMarker marker = (GeneticMarker) row.get(1);
            Integer id = gwasResult.getId();
            if (gwasResults.containsKey(id)) {
                gwasResultMarkers.get(id).add(marker);
            } else {
                gwasResults.put(id, gwasResult);
                Set<GeneticMarker> markers = new HashSet<>();
                markers.add(marker);
                gwasResultMarkers.put(id, markers);
            }
	}
        // store updated GWASResult objects with the associated markers
	osw.beginTransaction();
        try {
            for (Integer id : gwasResults.keySet()) {
                GWASResult gwasResult = PostProcessUtil.cloneInterMineObject(gwasResults.get(id));
                gwasResult.setFieldValue("markers", gwasResultMarkers.get(id));
		osw.store(gwasResult);
            }
        } catch (IllegalAccessException e) {
            throw new ObjectStoreException(e);
        }
        osw.commitTransaction();
        osw.close();
    }
}
