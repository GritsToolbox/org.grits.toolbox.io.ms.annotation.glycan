package org.grits.toolbox.io.ms.annotation.glycan.process.export;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.poi.ss.usermodel.Picture;
import org.apache.poi.ss.usermodel.Row;
import org.grits.toolbox.datamodel.ms.annotation.glycan.tablemodel.MSGlycanAnnotationTableDataObject;
import org.grits.toolbox.datamodel.ms.annotation.tablemodel.MSAnnotationTableDataObject;
import org.grits.toolbox.io.ms.annotation.listener.ExcelListener;
import org.grits.toolbox.io.ms.annotation.process.export.MSAnnotationWriterExcel;
import org.grits.toolbox.utils.image.GlycanImageProvider.GlycanImageObject;
import org.grits.toolbox.utils.io.ExcelWriterHelper;

/**
 * @author Brent Weatherly
 *
 */
/**
 * @author Brent Weatherly
 *
 */
public class MSGlycanAnnotationWriterExcel extends MSAnnotationWriterExcel
{
	protected List<Picture> m_images = new ArrayList<Picture>();
	protected ExcelWriterHelper helper = new ExcelWriterHelper();

	private static final Logger logger = Logger.getLogger(MSGlycanAnnotationWriterExcel.class);

	public void createNewFile(String a_strFilename, 
			MSAnnotationTableDataObject a_msAnnotationDataObject, int m_lastVisibleColInx,
			ExcelListener a_listener ) {
		super.createNewFile(a_strFilename, a_msAnnotationDataObject, m_lastVisibleColInx, a_listener);
		this.m_images = new ArrayList<Picture>();
	}

	protected MSGlycanAnnotationTableDataObject getMyTableDataObject() {
		return (MSGlycanAnnotationTableDataObject) getAnnotationDataObject();
	}

	public void close() throws IOException
	{
		for (Picture t_picture : this.m_images)
		{
			t_picture.resize();
		}		
		super.close();
	}

	@Override
	public void writeRow(int _iRow, boolean _bIsHidden) {
		super.writeRow(_iRow, _bIsHidden);
	}

	
	/**
	 * For standard annotation projects, the number of cartoon columns is the same as the number of sequence columns.
	 * 
	 * @return the offset in order to match the cartoon column to the sequence column
	 */
	protected int getCartoonColToSeqColOffset() {
		return 0;
	}
	
	@Override
	protected void writeCell( Row _excelRow, ArrayList<Object> _tableRow, int _iDataColNum, int _iPrefColNum, boolean _bIsHidden ) {
		if( _iPrefColNum < 0 ) 
			return;
		if ( _bIsHidden && _iDataColNum > m_lastVisibleColInx )
			return;    		
		if ( getMyTableDataObject().getCartoonCols().contains(_iDataColNum) ) {
			Object oCartoon = _tableRow.get(_iDataColNum);
			if( oCartoon == null ) {
				super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
				return;
			}
			int iInx = getMyTableDataObject().getCartoonCols().indexOf(_iDataColNum);
			iInx -= getCartoonColToSeqColOffset();
			if( iInx < 0 ) {
				iInx = getMyTableDataObject().getCartoonCols().indexOf(_iDataColNum);
				super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
				return;				
			}
			
			int iSeqColNum = getMyTableDataObject().getSequenceCols().get(iInx);
			String sSequence = (String) _tableRow.get(iSeqColNum);
			if( sSequence == null || sSequence.equals("")) {
				super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
				return;
			}
			writeCellImage(_excelRow, _tableRow, _iPrefColNum, sSequence);
		} else {
			super.writeCell(_excelRow, _tableRow, _iDataColNum, _iPrefColNum, _bIsHidden);
		}
	}

	protected void writeCellImage( Row _excelRow, ArrayList<Object> _tableRow, int _iPrefColNum, String _sSequence)
	{
		if ( _iPrefColNum < 0 || _sSequence == null )
			return;

		try 
		{	
			GlycanImageObject gio = MSGlycanAnnotationTableDataObject.glycanImageProvider.getImage(_sSequence);
			helper.writeCellImage(m_objWorkbook, m_objSheet, this.m_iRowCounter, _iPrefColNum, gio.getAwtBufferedImage(), m_images);	
		}
		catch (Exception e) 
		{
			this.errorMessage( "Image generation failed");
			logger.error(e.getMessage(), e);
		}
	}
}