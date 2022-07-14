package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.util.CellRangeAddress;
import org.grits.toolbox.datamodel.ms.annotation.glycan.tablemodel.MSGlycanAnnotationTableDataObject;
import org.grits.toolbox.display.control.table.datamodel.GRITSColumnHeader;
import org.grits.toolbox.utils.image.GlycanImageProvider.GlycanImageObject;

public class MSGlycanAnnotationSummaryWriterExcel extends MSGlycanAnnotationWriterExcel {
	
	//log4J Logger
	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationSummaryWriterExcel.class);
	private int iSizeRepeatingGrps=0;

	@Override
	protected void determineCollapsedColumnPositions() {
			
		ArrayList<GRITSColumnHeader> headerRow1 = getAnnotationDataObject().getTableHeader().get(0);
		ArrayList<GRITSColumnHeader> headerRow2 = getAnnotationDataObject().getTableHeader().get(1);
		int iFirstGrpInx = -1;
		int iLastGrpInx = -1;
		GRITSColumnHeader prevGrpHeader = null;
		int iCellCnt = headerRow1.size() - 1;
		for (int i = headerRow1.size() - 1; i >= 0; i--) {
			GRITSColumnHeader header1 = headerRow1.get(i);
			GRITSColumnHeader header2 = headerRow2.get(i);
			int iColNum = getAnnotationDataObject().getTablePreferences().getPreferenceSettings().getColumnPosition(header2);
			if (iColNum == -1) {
				continue;
			}
			
			if ( prevGrpHeader == null ) {
    			iFirstGrpInx = iCellCnt;
    			iLastGrpInx = iCellCnt;
    			prevGrpHeader = header1;
			} else if ( ! prevGrpHeader.equals(header1) ) {
				i = -1;
			} else {
				iLastGrpInx = iCellCnt;
			}
			iCellCnt--;
		}
		iSizeRepeatingGrps = iFirstGrpInx - iLastGrpInx + 1;
	
		
		List<List<Integer>> alPrefColNums = new ArrayList<>();
		List<Integer> alCurListOfCols = null;
		int iAdder = 0;
		int iPrevAdder = -1;
		HashMap<GRITSColumnHeader, Integer> htGroupToAdder = new HashMap<>();
		for (int i = 0; i < headerRow2.size(); i++) {
			GRITSColumnHeader header = headerRow2.get(i);
			int iColNum = getAnnotationDataObject().getTablePreferences().getPreferenceSettings().getColumnPosition(header);
			if (iColNum == -1) {
				continue;
			}
			if( htGroupToAdder.containsKey(header) ) {
				iAdder = htGroupToAdder.get(header) + iSizeRepeatingGrps;
			} else {
				iAdder = 0;
			}
			if( iAdder != iPrevAdder ) {
				alCurListOfCols = new ArrayList<>();
				alPrefColNums.add(alCurListOfCols);
				iPrevAdder = iAdder;
			}
			htGroupToAdder.put(header, iAdder);
			alCurListOfCols.add(iColNum + iAdder);
		}
		
		List<Integer> alFinalPrefColNums = new ArrayList<>();
		for( List<Integer> alListOfCols : alPrefColNums ) {
			Collections.sort(alListOfCols);
			alFinalPrefColNums.addAll(alListOfCols);
		}
		
		htGroupToAdder.clear();
		this.dataToPrefColumn = new HashMap<>();
		for (int i = 0; i < headerRow2.size(); i++) {
			GRITSColumnHeader header = headerRow2.get(i);
			int iColNum = getAnnotationDataObject().getTablePreferences().getPreferenceSettings().getColumnPosition(header);
			if (iColNum == -1) {
				continue;
			}
			if( htGroupToAdder.containsKey(header) ) {
				iAdder = htGroupToAdder.get(header) + iSizeRepeatingGrps;
			} else {
				iAdder = 0;
			}
			htGroupToAdder.put(header, iAdder);
			int iCollapsedColNum = alFinalPrefColNums.indexOf(iColNum + iAdder);
			dataToPrefColumn.put(i, iCollapsedColNum);
		}
	}
	
	@Override
	public void writeHeadline() {  	
		determineCollapsedColumnPositions();
		
      	Row t_row = this.m_objSheet.createRow(this.m_iRowCounter);
    	// first write top "grouped" row
		ArrayList<GRITSColumnHeader> headerRow1 = getAnnotationDataObject().getTableHeader().get(0);
		ArrayList<GRITSColumnHeader> headerRow2 = getAnnotationDataObject().getTableHeader().get(1);
		
		int iFirstGrpInx = -1;
		int iLastGrpInx = -1;
		GRITSColumnHeader prevGrpHeader = null;
		int iCellCnt = 0;
		for (int i = 0; i < headerRow1.size(); i++) {
			GRITSColumnHeader header1 = headerRow1.get(i);
			GRITSColumnHeader header2 = headerRow2.get(i);
			int iColNum = -1;
			if (getMyTableDataObject().getAnnotationIdCols().contains(i)) {
				iColNum = iCellCnt;
			}
			else iColNum = getAnnotationDataObject().getTablePreferences().getPreferenceSettings().getColumnPosition(header2);
			if (iColNum == -1) {
				continue;
			}
			Cell cell = t_row.createCell(iCellCnt);
			cell.setCellValue(header1.getLabel());
			cell.setCellType(CellType.STRING);
			if ( prevGrpHeader == null || ! prevGrpHeader.equals(header1) ) { // create cell
    			if( prevGrpHeader != null ) {  
    				if( iLastGrpInx == -1 || iLastGrpInx < iFirstGrpInx) {
    					iLastGrpInx = iFirstGrpInx; // must be only 1 cell in region
    				}
    				if (iLastGrpInx - iFirstGrpInx > 0)  // merged region should have at least 2 cells
    					this.m_objSheet.addMergedRegion(new CellRangeAddress(this.m_iRowCounter, this.m_iRowCounter, iFirstGrpInx, iLastGrpInx));
    			}
    			iFirstGrpInx = iCellCnt;
    			prevGrpHeader = header1;

			} else {
				iLastGrpInx = iCellCnt;
			}
			iCellCnt++;
		}
		if( prevGrpHeader != null ) {
			if( iLastGrpInx == -1 || iLastGrpInx < iFirstGrpInx) {
				iLastGrpInx = iFirstGrpInx; // must be only 1 cell in region
			}
			if (iLastGrpInx - iFirstGrpInx > 0)  // merged region should have at least 2 cells
				this.m_objSheet.addMergedRegion(new CellRangeAddress(this.m_iRowCounter, this.m_iRowCounter, iFirstGrpInx, iLastGrpInx));    				
		}
		
    	writeEmptyLine();
		t_row = this.m_objSheet.createRow(this.m_iRowCounter);
		ArrayList<GRITSColumnHeader> headerRow = (ArrayList<GRITSColumnHeader>) getAnnotationDataObject().getLastHeader();   
		int columnNo = 0;
		for (int i = 0; i < headerRow.size(); i++) {
			GRITSColumnHeader header = headerRow.get(i);
			if (getMyTableDataObject().getCartoonCols().contains(i) ) {
				Object cartoon = headerRow.get(i);
				if (cartoon != null) {
					writeCellImage(t_row, null, columnNo, ((GRITSColumnHeader)cartoon).getKeyValue());
					dataToPrefColumn.put(i, columnNo);
					columnNo++;
				}
			} else {
				int iColNum = getPreferredCellNumber(i);
				if( iColNum == -1 ) {
					continue;
				}
				Cell t_cell = t_row.createCell(iColNum);
				t_cell.setCellValue(header.getLabel());
				t_cell.setCellType(CellType.STRING);
				this.m_objSheet.setColumnWidth( iColNum, EXCEL_DEFAULT_COLUMN_WIDTH);
				columnNo++;
			}
		}

    	writeEmptyLine();
	}
	
	@Override
	protected void writeCell( Row _excelRow, ArrayList<Object> _tableRow, int _iDataColNum, int _iPrefColNum, boolean _bIsHidden ) {
		if( _bIsHidden) 
			return;   		
		if ( getMyTableDataObject().getCartoonCols().contains(_iDataColNum) ) {
			Object oCartoon = _tableRow.get(_iDataColNum);
			if( oCartoon == null ) {
				super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
				return;
			}
			int iInx = getMyTableDataObject().getCartoonCols().indexOf(_iDataColNum);
			int iSeqColNum = getMyTableDataObject().getSequenceCols().get(iInx);
			String sSequence = (String) _tableRow.get(iSeqColNum);
			writeCellImage(_excelRow, _tableRow, _iPrefColNum, sSequence);
			
		} else {
			if( _iPrefColNum < 0 ) 
				return;
			if (_iDataColNum > m_lastVisibleColInx )
				return; 
			super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
		}
	}
	
	protected void writeCellImage( Row _excelRow, ArrayList<Object> _tableRow, int _iPrefColNum, String _sSequence) {
		if ( _sSequence == null )
			return;
		
		try {	
			GlycanImageObject gio = MSGlycanAnnotationTableDataObject.glycanImageProvider.getImage(_sSequence);
			helper.writeCellImage(m_objWorkbook, m_objSheet, this.m_iRowCounter, _iPrefColNum, gio.getAwtBufferedImage(), m_images);	
		}
		catch (Exception e) {
			this.errorMessage( "Image generation failed");
			logger.error(e.getMessage(), e);
		}
	}
}
