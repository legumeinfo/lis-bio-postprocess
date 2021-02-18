package org.intermine.bio.postprocess;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.Chromosome;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.Location;
import org.intermine.model.bio.QTL;
import org.intermine.model.bio.QTLMarker;

import org.intermine.objectstore.ObjectStoreException;
import org.intermine.objectstore.ObjectStoreWriter;
import org.intermine.objectstore.query.ConstraintSet;
import org.intermine.objectstore.query.ContainsConstraint;
import org.intermine.objectstore.query.Query;
import org.intermine.objectstore.query.QueryClass;
import org.intermine.objectstore.query.QueryField;
import org.intermine.objectstore.query.QueryCollectionReference;
import org.intermine.objectstore.query.QueryObjectReference;
import org.intermine.objectstore.query.QueryValue;
import org.intermine.objectstore.query.Results;
import org.intermine.objectstore.query.ResultsRow;
import org.intermine.objectstore.query.SimpleConstraint;

import org.apache.log4j.Logger;

/**
 * Associate genes which are spanned by the genomic range of markers associated with QTLs with those QTLs. If only one marker, then it is simply the gene that overlaps that marker.
 *
 * @author Sam Hokin
 */
public class PopulateQTLGeneCollectionsProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateQTLGeneCollectionsProcess.class);

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateQTLGeneCollectionsProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     *
     * Main method
     *
     * @throws ObjectStoreException if the objectstore throws an exception
     */
    public void postProcess() throws ObjectStoreException {
        
        // ----------------------------------------------------------------------------------------------
        // First section - accumulate the QTLs and their genomic spans, from their associated markers
        // ----------------------------------------------------------------------------------------------

        LOG.info("Accumulating QTLs and genomic spans from QTLMarkers...");
        
        Query qQTL = new Query();
        qQTL.setDistinct(true);
        // 0 QTLMarker
        QueryClass qcQTLMarker = new QueryClass(QTLMarker.class);
        qQTL.addToSelect(qcQTLMarker);
        qQTL.addFrom(qcQTLMarker);

	// store each QTL's markers in a List
	Map<QTL,List<GeneticMarker>> qtlMarkerMap = new HashMap<>();

        // execute the query
	List<Object> qtlResultObjects = osw.getObjectStore().execute(qQTL).asList();
	for (Object qtlResultObject : qtlResultObjects) {
	    ResultsRow row = (ResultsRow) qtlResultObject;
	    QTLMarker qtlMarker = (QTLMarker) row.get(0);
	    QTL qtl = qtlMarker.getQtl();
	    GeneticMarker marker = qtlMarker.getMarker();
	    Location location = marker.getChromosomeLocation();
	    if (location==null) continue;
	    if (qtlMarkerMap.containsKey(qtl)) {
		List<GeneticMarker> markerList = qtlMarkerMap.get(qtl);
		markerList.add(marker);
	    } else {
		List<GeneticMarker> markerList = new LinkedList<>();
		markerList.add(marker);
		qtlMarkerMap.put(qtl, markerList);
	    }
	}

	// run through the QTLs to get the full genomic range of markers, load those into QTLSpan objects
	List<QTLSpan> qtlSpanList = new LinkedList<>();
	for (QTL qtl : qtlMarkerMap.keySet()) {
	    List<GeneticMarker> markerList = qtlMarkerMap.get(qtl);
	    int minStart = Integer.MAX_VALUE;
	    int maxEnd = 0;
	    Chromosome chromosome = null;
	    for (GeneticMarker marker : markerList) {
		Location location = marker.getChromosomeLocation();
		chromosome = (Chromosome) location.getLocatedOn();
		int start = location.getStart();
		int end = location.getEnd();
		if (start<minStart) minStart = start;
		if (end>maxEnd) maxEnd = end;
	    }
	    qtlSpanList.add(new QTLSpan(qtl, chromosome.getPrimaryIdentifier(), minStart, maxEnd));
	}

        // ----------------------------------------------------------------------------------
        // Second section: spin through the QTLs and query genes that fall within the QTLSpan
        // ----------------------------------------------------------------------------------

	// store spanned  genes per QTL in a map
	Map<QTL,Set<Gene>> qtlGeneMap = new HashMap<>();

	LOG.info("Spinning through QTLs, finding genes which they span...");
	for (QTLSpan qtlSpan : qtlSpanList) {
	    Query qGene = new Query();
	    qGene.setDistinct(false);
	    ConstraintSet csGene = new ConstraintSet(ConstraintOp.AND);
	    // 0 Gene
	    QueryClass qcGene = new QueryClass(Gene.class);
	    qGene.addFrom(qcGene);
	    qGene.addToSelect(qcGene);
	    // 1 Gene.chromosome
	    QueryClass qcGeneChromosome = new QueryClass(Chromosome.class);
	    qGene.addFrom(qcGeneChromosome);
	    qGene.addToSelect(qcGeneChromosome);
	    QueryObjectReference geneChromosome = new QueryObjectReference(qcGene, "chromosome");
	    csGene.addConstraint(new ContainsConstraint(geneChromosome, ConstraintOp.CONTAINS, qcGeneChromosome));
	    // 2 Gene.chromosomeLocation
	    QueryClass qcGeneLocation = new QueryClass(Location.class);
	    qGene.addFrom(qcGeneLocation);
	    qGene.addToSelect(qcGeneLocation);
	    QueryObjectReference geneLocation = new QueryObjectReference(qcGene, "chromosomeLocation");
	    csGene.addConstraint(new ContainsConstraint(geneLocation, ConstraintOp.CONTAINS, qcGeneLocation));
	    // require that the gene's chromosome equal that of QTLSpan
	    QueryValue qtlChromosomeIdValue = new QueryValue(qtlSpan.chromosomeId);
	    QueryField geneChromosomeIdField = new QueryField(qcGeneChromosome, "primaryIdentifier");
	    SimpleConstraint sameChromosomeConstraint = new SimpleConstraint(qtlChromosomeIdValue, ConstraintOp.EQUALS, geneChromosomeIdField);
	    csGene.addConstraint(sameChromosomeConstraint);
	    // require that the gene's location.end >= QTLSpan.start
	    QueryField geneLocationEndField = new QueryField(qcGeneLocation, "end");
	    QueryValue qtlSpanStartValue = new QueryValue(qtlSpan.start);
	    SimpleConstraint pastStartConstraint = new SimpleConstraint(geneLocationEndField, ConstraintOp.GREATER_THAN_EQUALS, qtlSpanStartValue);
	    csGene.addConstraint(pastStartConstraint);
	    // require that the gene's location.start <= QTLSpan.end
	    QueryField geneLocationStartField = new QueryField(qcGeneLocation, "start");
	    QueryValue qtlSpanEndValue = new QueryValue(qtlSpan.end);
	    SimpleConstraint beforeEndConstraint = new SimpleConstraint(geneLocationStartField, ConstraintOp.LESS_THAN_EQUALS, qtlSpanEndValue);
	    csGene.addConstraint(beforeEndConstraint);
	    // set the overall constraint
	    qGene.setConstraint(csGene);
	    
	    // store the genes in a Set
	    Set<Gene> geneSet = new HashSet<>();
	    // execute the query
	    List<Object> geneResultObjects = osw.getObjectStore().execute(qGene).asList();
	    for (Object geneResultObject : geneResultObjects) {
		ResultsRow row = (ResultsRow) geneResultObject;
		Gene gene = (Gene) row.get(0);
		geneSet.add(gene);
	    }
	    qtlGeneMap.put(qtlSpan.qtl, geneSet);
	}

	// store the genes with the QTLs
	osw.beginTransaction();
	for (QTL qtl : qtlGeneMap.keySet()) {
	    Set<Gene> geneSet = qtlGeneMap.get(qtl);
	    try {
		QTL tempQTL = PostProcessUtil.cloneInterMineObject(qtl);
		tempQTL.setFieldValue("spannedGenes", geneSet);
		osw.store(tempQTL);
	    } catch (IllegalAccessException e) {
		throw new RuntimeException(e);
	    }
	}
        osw.commitTransaction();
    }

    /**
     * Encapsulates a QTL and the genomic span: chromosome, start and end. Only need chromosome ID.
     */
    private class QTLSpan {

        QTL qtl;
        String chromosomeId;
        int start;
        int end;

        QTLSpan(QTL qtl, String chromosomeId, int start, int end) {
            this.qtl = qtl;
            this.chromosomeId = chromosomeId;
            this.start = start;
            this.end = end;
        }

	@Override
	public String toString() {
	    return qtl.getIdentifier()+" spans "+chromosomeId+":"+start+"-"+end;
	}
    }
        
}
