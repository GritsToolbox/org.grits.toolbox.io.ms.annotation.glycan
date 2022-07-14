package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.eurocarbdb.application.glycanbuilder.Glycan;
import org.eurocarbdb.resourcesdb.Config;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConversion;
import org.eurocarbdb.resourcesdb.io.MonosaccharideConverter;
import org.glycomedb.residuetranslator.ResidueTranslator;
import org.grits.toolbox.datamodel.ms.annotation.tablemodel.MSAnnotationTableDataObject;
import org.grits.toolbox.datamodel.ms.tablemodel.dmtranslate.DMPeak;
import org.grits.toolbox.datamodel.ms.tablemodel.dmtranslate.DMPrecursorPeak;
import org.grits.toolbox.display.control.table.datamodel.GRITSListDataRow;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationExcelListener;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationExportProcess;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationWriterExcel;
import org.grits.toolbox.ms.om.data.Feature;
import org.grits.toolbox.util.structure.glycan.filter.GlycanFilterOperator;
import org.grits.toolbox.util.structure.glycan.filter.om.FilterSetting;
import org.grits.toolbox.utils.image.GlycanImageProvider;
import org.grits.toolbox.utils.process.GlycoWorkbenchUtil;
import org.grits.toolbox.widgets.processDialog.ProgressDialog;
import org.grits.toolbox.widgets.progress.IProgressThreadHandler;

public class MSGlycanAnnotationExportProcess extends MSAnnotationExportProcess {	
	//log4J Logger
	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationExportProcess.class);
	
	protected FilterSetting filterSetting;
	protected GlycoWorkbenchUtil m_gwbUtil;

	protected String filterKey;
	protected double thresholdValue;
	protected int numTopHits = -1;
	
	@Override
	protected MSAnnotationWriterExcel getNewMSAnnotationWriterExcel() {
		return new MSGlycanAnnotationWriterExcel();
	}
	
	/**
	 * @param iRowNum
	 * @return 0 if visible and not hidden, 1 if hidden, 2 if invisible
	 */
	protected int isVisible( int iRowNum ) {
		GRITSListDataRow row = getTableDataObject().getTableData().get(iRowNum);
		int iParentScanIdCol = -1;
		if( getTableDataObject().getParentNoCol() != null && ! getTableDataObject().getParentNoCol().isEmpty() ) {
			iParentScanIdCol = getTableDataObject().getParentNoCol().get(0);
		}
		int iFeatureIdCol = getTableDataObject().getFeatureIdCols().get(0);

		Integer iParentScanNum = null;
		if( getMasterParentScan() != -1 ) {
			iParentScanNum = getMasterParentScan();
		} else if ( iParentScanIdCol != -1 && row.getDataRow().get(iParentScanIdCol) != null ) {
			iParentScanNum = (Integer) row.getDataRow().get(iParentScanIdCol);
		}
		if( iParentScanNum == null ) {
			return 0;
		}

		String sFeatureId = (String) row.getDataRow().get(iFeatureIdCol);
		if( sFeatureId == null && hideUnAnnotatedRows() ) {
			return 1;
		}
		
		String sRowId = getRowID(iRowNum);

		if( getTableDataObject().isHiddenRow(iParentScanNum, sRowId, sFeatureId) )  {
			return 1;
		}
		if( getTableDataObject().isInvisibleRow(iParentScanNum, sRowId) ) {
			return 2;
		}

		// if we make it here, then the experiment data is visible!
		return 0;
	}

	/**
	 * Updates the mAtLeastOne Map as to whether the at least one candidate structure is annotated 
	 * for the Row Id associated with the current Row Num (Row Id is determined from Peak Id and Scan Num
	 * and will be the same for all candidates. Row Num is the unique row number in the table).
	 * 
	 * @param iRowNum, the number of the row in the table
	 * @param mAtLeastOne, Map that tracks whether at least one candidate structure is selected for the Row Id associated w/ current Row Num
	 * @throws Exception
	 */
	protected void markAnnotatedRows(int iRowNum, Map<Integer, List<String>> mAtLeastOne) throws Exception  {
		GRITSListDataRow row = getTableDataObject().getTableData().get(iRowNum);

		int iPeakIdCol = getTableDataObject().getPeakIdCols().get(0);			
		Integer iPeakId = (Integer) row.getDataRow().get(iPeakIdCol);
		if( iPeakId == null ) {
			return;
		}

		int iFeatureIdCol = getTableDataObject().getFeatureIdCols().get(0);
		String sFeatureId = (String) row.getDataRow().get(iFeatureIdCol);
		if( sFeatureId == null && hideUnAnnotatedRows() ) {
			return;
		}

		String sRowId = getRowID(iRowNum);
		if( sFeatureId != null && iPeakId != null) {
			List<String> alAtLeastOne = null;
			if( ! mAtLeastOne.containsKey(iPeakId) ) {
				alAtLeastOne = new ArrayList<String>();
				mAtLeastOne.put(iPeakId, alAtLeastOne);
			} else {
				alAtLeastOne = mAtLeastOne.get(iPeakId);
			}
			alAtLeastOne.add(sRowId);			
		}
	}

	/**
	 * Based on the values stored in the mAtLeastOne map, determines if the row in the table, specified by
	 * iRowNum, is visible or not.
	 * 
	 * @param iRowNum, the number of the row in the table
	 * @param mAtLeastOne, Map that tracks whether at least one candidate structure is selected for the Row Id associated w/ current Row Num
	 * @return true if the row is annotated and isn't hidden
	 * @throws Exception
	 */
	protected boolean getFinalVisibility(int iRowNum, Map<Integer, List<String>> mAtLeastOne) throws Exception  {
		GRITSListDataRow row = getTableDataObject().getTableData().get(iRowNum);

		int iParentScanIdCol = -1;
		if( getTableDataObject().getParentNoCol() != null && ! getTableDataObject().getParentNoCol().isEmpty() ) {
			iParentScanIdCol = getTableDataObject().getParentNoCol().get(0);
		}
		Integer iParentScanNum = null;
		if( getMasterParentScan() != -1 ) {
			iParentScanNum = getMasterParentScan();
		} else if ( iParentScanIdCol != -1 && row.getDataRow().get(iParentScanIdCol) != null ) {
			iParentScanNum = (Integer) row.getDataRow().get(iParentScanIdCol);
		}
		if( iParentScanNum == null ) {
			return false;
		}

		int iPeakIdCol = getTableDataObject().getPeakIdCols().get(0);			
		Integer iPeakId = (Integer) row.getDataRow().get(iPeakIdCol);
		if( iPeakId == null ) {
			return false;
		}

		int iFeatureIdCol = getTableDataObject().getFeatureIdCols().get(0);
		String sFeatureId = (String) row.getDataRow().get(iFeatureIdCol);
		if( sFeatureId == null && hideUnAnnotatedRows() ) {
			return true;
		}
	
		String sRowId = getRowID(iRowNum);
		if( sRowId == null ) {
			logger.error("Null row id for row num: " + sRowId );
			return false;
		}

		if (iPeakId != null && !mAtLeastOne.containsKey(iPeakId)) {
			// there is no row for this parent scan
			// check if it was previously hidden, if so skip this row	
			if(  iPeakId != null && sFeatureId != null && iParentScanNum != null &&
					getTableDataObject().isHiddenRow(iParentScanNum, sRowId, sFeatureId) ) {
				return false;
			}
			return true;
		} else if (iPeakId != null){
			List<String> atLeastOne = mAtLeastOne.get(iPeakId);
			if (atLeastOne == null || atLeastOne.size() == 0) {
				// no rows for this parent scan
				// check if this is hidden already
				if(  iPeakId != null && sFeatureId != null && iParentScanNum != null &&
						getTableDataObject().isHiddenRow(iParentScanNum, sRowId, sFeatureId) ) {
					return false;
				}
				return true;
			}
		}									
		return false;
	}
	
	@Override
	public boolean threadStart(IProgressThreadHandler a_progressThreadHandler) throws Exception{
		try{
			// write values to Excel
			MSAnnotationWriterExcel t_writerExcel = getNewMSAnnotationWriterExcel();

			t_writerExcel.createNewFile(getOutputFile(), getTableDataObject(), getLastVisibleColInx(),
					new MSAnnotationExcelListener((ProgressDialog) a_progressThreadHandler));	
			
			Config t_objConf = new Config();
			MonosaccharideConversion t_msdb = new MonosaccharideConverter(t_objConf);
			try {
				m_gwbUtil = new GlycoWorkbenchUtil(new ResidueTranslator(), t_msdb);
			} catch (IOException e) {
				logger.error(e.getMessage(), e);
			}
			
			((ProgressDialog) a_progressThreadHandler).setMax(this.tableDataObject.getTableData().size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Processing data and filters");
			Map<Integer, Boolean> invisibleRowMap = new HashMap<>(); // map w/ row num as key
			Map<String, Double> rowIntensityMap = new HashMap<>(); // map w/ rowID as key
			Map<String, Double> topRowIntensityMap = new HashMap<>(); // map w/ rowID as key
			Map<Integer, Boolean> rowGlycanMap = new HashMap<>(); // map w/ row num as key
			Map<Integer, List<String>> mAtLeastOne = new HashMap<>();
								
			Map<String, List<Integer>> mRowIdToRowNum = getRowIdtoRowNumMap();
			
			for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
				if(isCanceled()) {
					t_writerExcel.close();
					return false;
				}
				
				GRITSListDataRow row = getTableDataObject().getTableData().get(i);
				
				int iVisInfo = isVisible(i);
				
				// need to keep track of invisible rows for writing into excel later
				if( iVisInfo == 1 ) {
					continue;
				}
				invisibleRowMap.put(i, (iVisInfo == 2));
								
				Boolean passedGlycan = applyGlycanFilters (row.getDataRow());
				rowGlycanMap.put(i, passedGlycan);
				
				// check for the other filters (threshold value)
				applyIntensityFilter(i, rowIntensityMap);					
				
				markAnnotatedRows(i, mAtLeastOne );
				
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (i+1));
			}
			
			topRowIntensityMap = applyTopHits (rowIntensityMap, numTopHits);
			
			((ProgressDialog) a_progressThreadHandler).setMax(getTableDataObject().getTableData().size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Processing filter results");
			// go over the table once more to see if any scans are completely hidden because of the filters
			for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
				if(isCanceled()) {
					t_writerExcel.close();
					return false;
				}
				boolean bIsInVis = getFinalVisibility( i, mAtLeastOne );
				if( bIsInVis ) {
					invisibleRowMap.put(i, true);
				}
				
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (i+1));
			}
				
			((ProgressDialog) a_progressThreadHandler).setMax(topRowIntensityMap.size());
			((ProgressDialog) a_progressThreadHandler).setProcessMessageLabel("Exporting data");
			List<Integer> exportRows = new ArrayList<Integer>();
			int i=0;
			for (String rowId : mRowIdToRowNum.keySet()) {
				if( ! topRowIntensityMap.containsKey(rowId) ) {
					continue;
				}
				List<Integer> tableRows = null;
				if( mRowIdToRowNum.containsKey(rowId) ) {
					tableRows = mRowIdToRowNum.get(rowId);
				}
				if( tableRows == null  ) {
					continue;
				}
				for( Integer rowNum : tableRows ) {
					if( ! rowGlycanMap.isEmpty() && ( ! rowGlycanMap.containsKey(rowNum) || ! rowGlycanMap.get(rowNum) ) ) {
						continue; 
					}
					if( ! invisibleRowMap.containsKey(rowNum) ) {					
						continue;
					}
					exportRows.add(rowNum);
				}
			}
			Collections.sort(exportRows);
			for (Integer rowNum : exportRows ) {
				//write scan
				logger.debug("Export row num: " + rowNum);
				t_writerExcel.writeRow(rowNum, invisibleRowMap.get(rowNum));
				//show in dialog
				((ProgressDialog) a_progressThreadHandler).updateProgresBar("Scan: "+ (i+1));
				i++;				
			}
			t_writerExcel.close();
		}catch(Exception e)
		{
			logger.error(e.getMessage(), e);
			throw e;
		}
		return true;
	}
	
	/**
	 * If the user specified a maximum number of annotated rows to display, this method sorts
	 * the rowIntensityMap by intensity (descending) and then sets the values in the topSelectedRows map
	 * with the correct number of rows to display. 
	 * 
	 * @param rowIntensityMap, map of the peaks to their intensities (may have been set to 0 if filtered out for some other reason)
	 * @param numTopHits, the number of annotated rows to display
	 * @return a Map of the Row Ids to the Row Numbers to display after filter (if performed)
	 */
	protected Map<String, Double> applyTopHits(Map<String, Double> rowIntensityMap, int numTopHits) {
		if( numTopHits <= 0 || filterKey == null ) {
			return rowIntensityMap;
		}
		Map<String, Double> sortedMap = sortByValue(rowIntensityMap);
		Map<String, Double> topSelectedRows = new LinkedHashMap<>();  // keep the order
		int i=0;
		// if there are more than 1 row w/ same key, pull out the "numTopHits" rows
		for (String rowId: sortedMap.keySet()) {
			if (i < numTopHits) {
				topSelectedRows.put(rowId, sortedMap.get(rowId));
			} else { 
				break;
			}
			i++;
		}
		return topSelectedRows;
	}

	/**
	 * Because this is a Glycan Annotation table, this method assumes a single cartoon and a single sequence.
	 * This method will call "passesFilter" method to verify if the sequence passes the specified glycan filters
	 * (if any).
	 * 
	 * @param dataRow, array of values in the current row
	 * @return true if the sequence passes any specified glycan filter (or none specified). Returns false otherwise.
	 * @throws Exception
	 * 
	 */
	protected Boolean applyGlycanFilters(ArrayList<Object> dataRow) throws Exception {
		// check if the row passes the filters
		int sequenceCol = getTableDataObject().getSequenceCols().get(0);
		String sequence = (String) dataRow.get(sequenceCol);
		boolean bPasses = passesFilters (sequence);
		if( ! bPasses ) {
			int iFeatureIdCol = getTableDataObject().getFeatureIdCols().get(0);
			String sFeatureId = (String) dataRow.get(iFeatureIdCol);
			logger.debug(sFeatureId + " failed to pass the filter. Skipping!");			
		}		
		return bPasses;
	}
	
	/**
	 * Because a particular peak may have multiple candidates, there is a key that specifies the unique key
	 * for that Peak + Scan combination (it is possible for a MS peak to generate multiple scans). This method
	 * returns the Row Id for the particular Row Number of the table.
	 * 
	 * @param iRowNum, the number in the table for a particular row
	 * @return the Row Id for the specified Row Number
	 */
	protected String getRowID( int iRowNum ) {
		GRITSListDataRow row = getTableDataObject().getTableData().get(iRowNum);
		int iPeakIdCol = getTableDataObject().getPeakIdCols().get(0);			
		Integer iPeakId = (Integer) row.getDataRow().get(iPeakIdCol);
		if( iPeakId == null ) {
			return String.valueOf(iRowNum);
		}
		int iScanIdCol = -1;
		if( getTableDataObject().getScanNoCols() != null && ! getTableDataObject().getScanNoCols().isEmpty() ) {
			iScanIdCol = getTableDataObject().getScanNoCols().get(0);
		}
		Integer iScanNum = null;
		if (iScanIdCol != -1) {
			iScanNum = (Integer) row.getDataRow().get(iScanIdCol);
		}
		
		String sRowId = Feature.getRowId(iPeakId, iScanNum, ((MSAnnotationTableDataObject) getTableDataObject()).getUsesComplexRowId());
		if( sRowId == null ) {
			logger.error("Null row id for row num: " + iRowNum);
			return String.valueOf(iRowNum); // this should never happen
		}		
		return sRowId;		
	}
	
	/**
	 * Generates a Map of the Row ID to the list of Row Numbers in the table (basically all candidate structures for 
	 * the particular Peak + Scan combination)
	 * 
	 * @return Map of Row ID to list of Row Numbers in table
	 */
	protected Map<String, List<Integer>> getRowIdtoRowNumMap() {
		Map<String, List<Integer>> mRowIdToRunNum = new HashMap<>();
		for( int i = 0; i < getTableDataObject().getTableData().size(); i++ )  {
			if(isCanceled()) {
				return null;
			}
			String sRowID = getRowID(i);
			List<Integer> lRowNums = null;
			if( mRowIdToRunNum.containsKey(sRowID) ) {
				lRowNums = mRowIdToRunNum.get(sRowID);
			} else {
				lRowNums = new ArrayList<>();
				mRowIdToRunNum.put(sRowID, lRowNums);				
			}
			lRowNums.add(i);			
		}		
		return mRowIdToRunNum;
	}
	
	/**
	 * If the user specifies a minimum peak or precursor intensity, this method filters out those peaks
	 * whose intensity is less than the specified threshold by setting their intensity values to 0.
	 * 
	 * @param iRowNum
	 * @param rowIntensityMap
	 */
	protected void applyIntensityFilter(int iRowNum, Map<String, Double> rowIntensityMap) {
		GRITSListDataRow row = getTableDataObject().getTableData().get(iRowNum);
		String sRowId = getRowID(iRowNum);
		// duplicate row, no need to retest or add
		if( rowIntensityMap.containsKey(sRowId) ) {
			return;
		}
		Double dPeakIntensity = 0.0;
		if (filterKey != null && filterKey.equals(DMPeak.peak_intensity.getLabel())) {
			Double peakIntensity = (Double)row.getDataRow().get(getTableDataObject().getPeakIntensityCols().get(0));
			if (this.thresholdValue > 0 && peakIntensity >= this.thresholdValue ) { // skip this row since it fails to pass the threshold filter
				dPeakIntensity = peakIntensity;
			}
		} else if (filterKey != null && filterKey.equals(DMPrecursorPeak.precursor_peak_intensity.getLabel())) {
			Double intensity = (Double)row.getDataRow().get(getTableDataObject().getPrecursorIntensityCols().get(0));
			if (this.thresholdValue > 0 && intensity >= this.thresholdValue) { // skip this row since it fails to pass the threshold filter
				dPeakIntensity = intensity;
			}
		}
		rowIntensityMap.put(sRowId, dPeakIntensity);
	}

	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map) {
	    return map.entrySet()
	              .stream()
	              .sorted(Map.Entry.comparingByValue(Collections.reverseOrder()))
	              .collect(Collectors.toMap(
	                Map.Entry::getKey, 
	                Map.Entry::getValue, 
	                (e1, e2) -> e1, 
	                LinkedHashMap::new
	              ));
	}

	/**
	 * On the Annotation viewer, the user can specify a variety of filters to search for glycan features (or remove). 
	 * These filters are stored in "filterSetting" object. This method checks to see if the filterSetting isn't null, 
	 * and if not, then parses the sequence as necessary to verify if it passes the specified filters.
	 * 
	 * @param sequence, Glycoworkbench string representation of a glycan
	 * @return true if sequence passes user-specified glycan filters (or no filter present). False otherwise
	 * @throws Exception
	 */
	protected boolean passesFilters(String sequence) throws Exception {
		if (sequence != null && !sequence.isEmpty() && filterSetting != null) {
			int iInx1 = sequence.indexOf(GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR);
			if (iInx1 < 0) {  // single sequence
				m_gwbUtil.parseGWSSequence(sequence);
				Glycan glycan = m_gwbUtil.getGlycoWorkbenchGlycan();
				return GlycanFilterOperator.evaluate(glycan.toSugar(), filterSetting.getFilter());
			} else { // if any of the sequences passes the filter, return true
				String sRemaining = sequence;
				do {
					String sSeq = iInx1 > 0 ? sRemaining.substring(0, iInx1) : sRemaining;
					m_gwbUtil.parseGWSSequence(sSeq);
					Glycan glycan = m_gwbUtil.getGlycoWorkbenchGlycan();
					if (GlycanFilterOperator.evaluate(glycan.toSugar(), filterSetting.getFilter()))  // even if one satisfies the filter, the row will be included
						return true;
					sRemaining = iInx1 > 0 ? sRemaining.substring(iInx1	+ GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR.length()) : null;
					iInx1 = sRemaining != null ? sRemaining.indexOf(GlycanImageProvider.COMBO_SEQUENCE_SEPARATOR) : -1;
				} while (sRemaining != null);
			}
		} 
		// for now allow empty sequences (unannotated rows) to pass the filter
		// TODO decide based on "hideUnAnnotatedPeaks" 
		return true;
	}

	public void setFilterSetting(FilterSetting filterSetting) {
		this.filterSetting = filterSetting;
	}
	
	public void setFilterKey(String filterKey) {
		this.filterKey = filterKey;
	}
	
	public void setNumTopHits(int numTopHits) {
		this.numTopHits = numTopHits;
	}
	
	public void setThresholdValue(double thresholdValue) {
		this.thresholdValue = thresholdValue;
	}
		
}
