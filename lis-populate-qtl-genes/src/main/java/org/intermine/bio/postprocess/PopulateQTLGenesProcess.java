package org.intermine.bio.postprocess;

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

        // query all QTLs
        Query qQTL = new Query();
        QueryClass qcQTL = new QueryClass(QTL.class);
        qQTL.addToSelect(qcQTL);
        qQTL.addFrom(qcQTL);

        // load a Set of GeneticMarkers into a Map keyed by QTL
	Map<QTL,Set<GeneticMarker>> qtlMarkersMap = new HashMap<>();
        int count = 0;
        Results qtlResults = osw.getObjectStore().execute(qQTL);
        for (Object obj : qtlResults.asList()) {
            ResultsRow row = (ResultsRow) obj;
	    QTL qtl = (QTL) row.get(0);
            for (GeneticMarker marker : qtl.getMarkers()) {
                if (marker.getChromosomeLocation() == null) continue; // marker not mapped to a chromosome
                if (qtlMarkersMap.containsKey(qtl)) {
                    qtlMarkersMap.get(qtl).add(marker);
                } else {
                    Set<GeneticMarker> markers = new HashSet<>();
                    markers.add(marker);
                    qtlMarkersMap.put(qtl, markers);
                }
            }
            count++;
	}
        LOG.info("Found " + count + " genetic markers associated with " + qtlMarkersMap.size() + " QTLs.");
        
	// run through the QTLs to get the full genomic range of markers, load those into QTLSpan objects
	Set<QTLSpan> qtlSpans = new HashSet<>();
        for (QTL qtl : qtlMarkersMap.keySet()) {
            Set<GeneticMarker> markers = qtlMarkersMap.get(qtl);
            int minStart = Integer.MAX_VALUE;
            int maxEnd = 0;
            String chromosomeIdentifier = null;
            Set<Chromosome> chromosomes = new HashSet<>(); // for checking single chromosome
            for (GeneticMarker marker : markers) {
                Location location = marker.getChromosomeLocation();
                Chromosome chromosome = (Chromosome) location.getLocatedOn();
                chromosomeIdentifier = chromosome.getPrimaryIdentifier();
                chromosomes.add(chromosome);
                int start = location.getStart();
                int end = location.getEnd();
                if (start < minStart) minStart = start;
                if (end > maxEnd) maxEnd = end;
            }
            // check that we're on the same chromosome!
            if (chromosomes.size() > 1) {
                LOG.warn("QTL " + qtl.getPrimaryIdentifier() + " has markers on different chromosomes: ");
                for (GeneticMarker mrk : qtlMarkersMap.get(qtl)) {
                    LOG.warn(mrk.getPrimaryIdentifier() + "\t" + mrk.getName() + "\t" + mrk.getChromosome().getPrimaryIdentifier());
                }
            } else {
                qtlSpans.add(new QTLSpan(qtl, chromosomeIdentifier, minStart, maxEnd));
            }
        }
        LOG.info("Found " + qtlSpans.size() + " QTL spans to search for spanned genes.");

        // ----------------------------------------------------------------------------------
        // Second section: spin through the QTLs and query genes that fall within the QTLSpan
        // ----------------------------------------------------------------------------------

	// store spanned genes per QTL in a map
	Map<QTL,Set<Gene>> qtlGeneMap = new HashMap<>();
        count = 0;
	for (QTLSpan qtlSpan : qtlSpans) {

	    Query qGene = new Query();
	    qGene.setDistinct(false);
	    // 0 Gene
	    QueryClass qcGene = new QueryClass(Gene.class);
	    qGene.addFrom(qcGene);
	    qGene.addToSelect(qcGene);
	    // Chromosome
	    QueryClass qcChromosome = new QueryClass(Chromosome.class);
	    qGene.addFrom(qcChromosome);
	    // Location
	    QueryClass qcLocation = new QueryClass(Location.class);
	    qGene.addFrom(qcLocation);

	    ConstraintSet cs = new ConstraintSet(ConstraintOp.AND);
            // Gene.chromosome = Chromosome
	    QueryObjectReference geneChromosome = new QueryObjectReference(qcGene, "chromosome");
	    cs.addConstraint(new ContainsConstraint(geneChromosome, ConstraintOp.CONTAINS, qcChromosome));
            // Gene.location = Location
	    QueryObjectReference geneChromosomeLocation = new QueryObjectReference(qcGene, "chromosomeLocation");
	    cs.addConstraint(new ContainsConstraint(geneChromosomeLocation, ConstraintOp.CONTAINS, qcLocation));
	    // QTLSpan.chromosome = Chromosome
	    QueryField chromosomePrimaryIdentifier = new QueryField(qcChromosome, "primaryIdentifier");
	    QueryValue qtlSpanChromosomeId = new QueryValue(qtlSpan.chromosomeId);
            cs.addConstraint(new SimpleConstraint(chromosomePrimaryIdentifier, ConstraintOp.EQUALS, qtlSpanChromosomeId));
	    // Location.end >= QTLSpan.start
	    QueryField locationEnd = new QueryField(qcLocation, "end");
	    QueryValue qtlSpanStart = new QueryValue(qtlSpan.start);
            cs.addConstraint(new SimpleConstraint(locationEnd, ConstraintOp.GREATER_THAN_EQUALS, qtlSpanStart));
	    // Location.start <= QTLSpan.end
	    QueryField locationStart = new QueryField(qcLocation, "start");
	    QueryValue qtlSpanEnd = new QueryValue(qtlSpan.end);
            cs.addConstraint(new SimpleConstraint(locationStart, ConstraintOp.LESS_THAN_EQUALS, qtlSpanEnd));
	    // set the overall constraint
	    qGene.setConstraint(cs);

	    // store this QTL's spanned genes in a Set
	    Set<Gene> genes = new HashSet<>();
            Results geneResults = osw.getObjectStore().execute(qGene);
            for (Object obj : geneResults.asList()) {
		ResultsRow row = (ResultsRow) obj;
		Gene gene = (Gene) row.get(0);
		genes.add(gene);
                count++;
	    }
	    qtlGeneMap.put(qtlSpan.qtl, genes);
	}
        LOG.info("Found " + count + " genes spanned by " + qtlGeneMap.size() + " QTLs.");

	// store the QTL.genes collections
	osw.beginTransaction();
	for (QTL qtl : qtlGeneMap.keySet()) {
	    Set<Gene> genes = qtlGeneMap.get(qtl);
            QTL clone = PostProcessUtil.cloneInterMineObject(qtl);
            clone.setFieldValue("genes", genes);
            osw.store(clone);
	}
        osw.commitTransaction();
        LOG.info("Stored genes collections for " + qtlGeneMap.size() + " QTLs.");
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
