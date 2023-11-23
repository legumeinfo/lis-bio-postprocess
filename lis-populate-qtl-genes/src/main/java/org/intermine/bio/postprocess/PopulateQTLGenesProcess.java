package org.intermine.bio.postprocess;

import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.ConcurrentSkipListSet;

import org.intermine.bio.util.PostProcessUtil;
import org.intermine.postprocess.PostProcessor;
import org.intermine.metadata.ConstraintOp;

import org.intermine.model.bio.Chromosome;
import org.intermine.model.bio.Gene;
import org.intermine.model.bio.GeneticMarker;
import org.intermine.model.bio.Location;
import org.intermine.model.bio.QTL;

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
 * Associate genes which are spanned by the genomic range of markers associated with QTLs with those QTLs.
 * If only one marker, then it is simply the gene that overlaps that marker.
 *
 * NOTE: this post-processor must be run AFTER the QTL.markers collections have been populated!
 *
 * @author Sam Hokin
 */
public class PopulateQTLGenesProcess extends PostProcessor {

    private static final Logger LOG = Logger.getLogger(PopulateQTLGenesProcess.class);

    /**
     * Construct with an ObjectStoreWriter, read and write from the same ObjectStore
     * @param osw object store writer
     */
    public PopulateQTLGenesProcess(ObjectStoreWriter osw) {
        super(osw);
    }

    /**
     * {@inheritDoc}
     */
    public void postProcess() throws ObjectStoreException, IllegalAccessException {
        
        // ----------------------------------------------------------------------------------------------
        // First section - accumulate the QTLs and their genomic spans, from their associated markers
        // ----------------------------------------------------------------------------------------------

        LOG.info("Querying QTLs...");
	// store each QTL's markers in a Set in a Map keyed by QTL
	Map<QTL,Set<GeneticMarker>> qtlMarkersMap = new HashMap<>();
        Query qQTL = new Query();
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addToSelect(qcQTL);
        qQTL.addFrom(qcQTL);
        // execute the query
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        for (Object obj : qtlResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
	    QTL qtl = (QTL) row.get(0);
            for (GeneticMarker marker : qtl.getMarkers()) {
                Location location = marker.getChromosomeLocation();
                if (location == null) continue; // unlocated marker
                if (qtlMarkersMap.containsKey(qtl)) {
                    qtlMarkersMap.get(qtl).add(marker);
                } else {
                    Set<GeneticMarker> markers = new HashSet<>();
                    markers.add(marker);
                    qtlMarkersMap.put(qtl, markers);
                }
            }
	}
        LOG.info("Found " + qtlMarkersMap.size() + " QTL objects.");
        
	LOG.info("Spinning through QTLs, finding their genomic spans...");
        /////////////////////////////////////////////////////////////////////////////////////////////////
	// run through the QTLs to get the full genomic range of markers, load those into QTLSpan objects
	Set<QTLSpan> qtlSpans = new ConcurrentSkipListSet<>();
        qtlMarkersMap.keySet().parallelStream().forEach(qtl -> {
                Set<GeneticMarker> markers = qtlMarkersMap.get(qtl);
                int minStart = Integer.MAX_VALUE;
                int maxEnd = 0;
                Chromosome chromosome = null;
                for (GeneticMarker marker : markers) {
                    Location location = marker.getChromosomeLocation();
                    chromosome = (Chromosome) location.getLocatedOn();
                    int start = location.getStart();
                    int end = location.getEnd();
                    if (start < minStart) minStart = start;
                    if (end > maxEnd) maxEnd = end;
                }
                qtlSpans.add(new QTLSpan(qtl, chromosome.getPrimaryIdentifier(), minStart, maxEnd));
            });
        /////////////////////////////////////////////////////////////////////////////////////////////////
        LOG.info("Found " + qtlSpans.size() + " QTL spans.");

        // ----------------------------------------------------------------------------------
        // Second section: spin through the QTLs and query genes that fall within the QTLSpan
        // ----------------------------------------------------------------------------------

	LOG.info("Spinning through QTLs, finding the genes within their spans...");
	// store spanned genes per QTL in a map
	Map<QTL,Set<Gene>> qtlGeneMap = new HashMap<>();
	for (QTLSpan qtlSpan : qtlSpans) {
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
	    // store this QTL's spanned genes in a Set
	    Set<Gene> genes = new HashSet<>();
	    // execute the query
            Results geneResults = osw.getObjectStore().execute(qGene);
            for (Object obj : geneResults.asList()) {
		ResultsRow row = (ResultsRow) obj;
		Gene gene = (Gene) row.get(0);
		genes.add(gene);
	    }
	    qtlGeneMap.put(qtlSpan.qtl, genes);
	}
        LOG.info("Found associated genes for " + qtlGeneMap.size() + " QTLs.");

	// store the QTL.genes collections
	osw.beginTransaction();
	for (QTL qtl : qtlGeneMap.keySet()) {
	    Set<Gene> genes = qtlGeneMap.get(qtl);
            QTL clone = PostProcessUtil.cloneInterMineObject(qtl);
            clone.setFieldValue("genes", genes);
            osw.store(clone);
	}
        osw.commitTransaction();
    }

    /**
     * Encapsulates a QTL and the genomic span: chromosome, start and end. Only need chromosome ID.
     */
    private class QTLSpan implements Comparable {
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
	    return qtl.getPrimaryIdentifier() + " spans " + chromosomeId + ":" + start + "-" + end;
	}

        @Override
        public int compareTo(Object o) {
            QTLSpan that = (QTLSpan) o;
            if (this.qtl.getName().equals(that.qtl.getName())) {
                if (this.chromosomeId.equals(that.chromosomeId)) {
                    if (this.start == that.start) {
                        return this.end - that.end;
                    } else {
                        return this.start - that.start;
                    }
                } else {
                    return this.chromosomeId.compareTo(that.chromosomeId);
                }
            } else {
                return this.qtl.getName().compareTo(that.qtl.getName());
            }
        }
    }
}
