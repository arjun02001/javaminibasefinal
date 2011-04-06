package bitmap;

import java.io.IOException;

import global.Convert;
import global.GlobalConst;
import global.PageId;
import global.RID;
import diskmgr.Page;

/**
 * Class Bit Map page. The design assumes that records are kept compacted when
 * deletions are performed.
 */

public class BMPage extends Page implements GlobalConst {

	public static final int DPFIXED = 3 * 2 + 3 * 4;
	public static final int SLOT_CNT = 0;
	public static final int USED_PTR = 2;
	public static final int FREE_SPACE = 4;
	public static final int PREV_PAGE = 6;
	public static final int NEXT_PAGE = 10;
	public static final int CUR_PAGE = 14;

	/*
	 * Warning: These items must all pack tight, (no padding) for the current
	 * implementation to work properly. Be careful when modifying this class.
	 */
	
	/**
	   * number of slots in use
	   */
	  private    short     slotCnt; 

	/**
	 * offset of first used byte by data records in data[]
	 */
	private short usedPtr;

	/**
	 * number of bytes free in data[]
	 */
	private short freeSpace;

	/**
	 * backward pointer to data page
	 */
	protected PageId prevPage = new PageId();

	/**
	 * forward pointer to data page
	 */
	protected PageId nextPage = new PageId();

	/**
	 * page number of this page
	 */
	protected PageId curPage = new PageId();

	/**
	 * Default constructor
	 */

	public BMPage() {
	}

	/**
	 * Constructor of class HFPage open a HFPage and make this HFpage piont to
	 * the given page
	 * 
	 * @param page
	 *            the given page in Page type
	 */

	public BMPage(Page page) {
		data = page.getpage();
	}

	/**
	 * Constructor of class BMPage open a existed bmpage
	 * 
	 * @param apage
	 *            a page in buffer pool
	 */

	public void openBMpage(Page apage) {
		data = apage.getpage();
	}

	/**
	 * Constructor of class BMPage initialize a new page
	 * 
	 * @param pageNo
	 *            the page number of a new page to be initialized
	 * @param apage
	 *            the Page to be initialized
	 * @see Page
	 * @exception IOException
	 *                I/O errors
	 */

	public void init(PageId pageNo, Page apage) throws IOException {
		data = apage.getpage();
		
		slotCnt = 0;                // no slots in use
	    Convert.setShortValue (slotCnt, SLOT_CNT, data);

		curPage.pid = pageNo.pid;
		Convert.setIntValue(curPage.pid, CUR_PAGE, data);

		nextPage.pid = INVALID_PAGE;
		prevPage.pid = INVALID_PAGE;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);

		usedPtr = (short) MAX_SPACE; // offset in data array (grow backwards)
		Convert.setShortValue(usedPtr, USED_PTR, data);

		freeSpace = (short) (MAX_SPACE - DPFIXED); // amount of space available
		Convert.setShortValue(freeSpace, FREE_SPACE, data);
	}

	/**
	 * @return byte array
	 */

	public byte[] getBMpageArray() {
		return data;
	}

	public void writeBMPageArray(byte[] record) throws IOException {
		int recLen = record.length;
		//System.out.println("Write BMPage Array length is : "+record.length);
		int spaceNeeded = recLen;
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		if (spaceNeeded <= freeSpace) {
			// adjust free space
			freeSpace -= spaceNeeded;
			Convert.setShortValue(freeSpace, FREE_SPACE, data);
			usedPtr = Convert.getShortValue(USED_PTR, data);
			usedPtr -= recLen; // adjust usedPtr
			Convert.setShortValue(usedPtr, USED_PTR, data);
			// insert data onto the data page
			System.arraycopy(record, 0, data, usedPtr, recLen);
			curPage.pid = Convert.getIntValue(CUR_PAGE, data);
		}
	}

	

	/**
	 * returns the amount of available space on the page.
	 * 
	 * @return the amount of available space on the page
	 * @exception IOException
	 *                I/O errors
	 */
	public int available_space() throws IOException {
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		return (freeSpace);
	}

	/**
	 * Dump contents of a page
	 * 
	 * @exception IOException
	 *                I/O errors
	 */
	public void dumpPage() throws IOException {
		int i, n;

		curPage.pid = Convert.getIntValue(CUR_PAGE, data);
		nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
		usedPtr = Convert.getShortValue(USED_PTR, data);
		freeSpace = Convert.getShortValue(FREE_SPACE, data);
		slotCnt =  Convert.getShortValue (SLOT_CNT, data);
		
		System.out.println("dumpPage");
		System.out.println("curPage= " + curPage.pid);
		System.out.println("nextPage= " + nextPage.pid);
		System.out.println("usedPtr= " + usedPtr);
		System.out.println("freeSpace= " + freeSpace);
		System.out.println("slotCnt= " + slotCnt);
	}

	/**
	 * Determining if the page is empty
	 * 
	 * @return true if the BMPage is has no records in it, false otherwise
	 * @exception IOException
	 *                I/O errors
	 */
	public boolean empty() throws IOException {
		if (MAX_SPACE - DPFIXED == freeSpace) {
			return true;
		}
		return false;
	}

	/**
	 * @return page number of current page
	 * @exception IOException
	 *                I/O errors
	 */
	public PageId getCurPage() throws IOException {
		curPage.pid = Convert.getIntValue(CUR_PAGE, data);
		return curPage;
	}

	/**
	 * sets value of curPage to pageNo
	 * 
	 * @param pageNo
	 *            page number for current page
	 * @exception IOException
	 *                I/O errors
	 */
	public void setCurPage(PageId pageNo) throws IOException {
		curPage.pid = pageNo.pid;
		Convert.setIntValue(curPage.pid, CUR_PAGE, data);
	}

	/**
	 * @return page number of next page
	 * @exception IOException
	 *                I/O errors
	 */
	public PageId getNextPage() throws IOException {
		nextPage.pid = Convert.getIntValue(NEXT_PAGE, data);
		return nextPage;
	}

	/**
	 * sets value of nextPage to pageNo
	 * 
	 * @param pageNo
	 *            page number for next page
	 * @exception IOException
	 *                I/O errors
	 */
	public void setNextPage(PageId pageNo) throws IOException {
		nextPage.pid = pageNo.pid;
		Convert.setIntValue(nextPage.pid, NEXT_PAGE, data);
	}

	/**
	 * @return PageId of previous page
	 * @exception IOException
	 *                I/O errors
	 */
	public PageId getPrevPage() throws IOException {
		prevPage.pid = Convert.getIntValue(PREV_PAGE, data);
		return prevPage;
	}

	/**
	 * sets value of prevPage to pageNo
	 * 
	 * @param pageNo
	 *            page number for previous page
	 * @exception IOException
	 *                I/O errors
	 */
	public void setPrevPage(PageId pageNo) throws IOException {
		prevPage.pid = pageNo.pid;
		Convert.setIntValue(prevPage.pid, PREV_PAGE, data);
	}
	
	 /**
	   * @return 	slotCnt used in this page
	   * @exception IOException I/O errors
	   */
	  public short getSlotCnt() 
	    throws IOException
	    {
	      return Convert.getShortValue(SLOT_CNT, data);
	    }
	  
	  /**
	   * sets slot contents
	   * @param       slotno  the slot number 
	   * @param 	length  length of record the slot contains
	   * @param	offset  offset of record
	   * @exception IOException I/O errors
	   */
	  public void setSlot(int slotCnt)
	    throws IOException
	    {
		  this.slotCnt = (short) slotCnt;
		  Convert.setShortValue(this.slotCnt, SLOT_CNT, data);
	    }
	  

}
