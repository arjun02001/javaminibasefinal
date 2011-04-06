package bitmap;

import java.io.IOException;

import btree.ConstructPageException;
import diskmgr.Page;

import global.PageId;
import global.SystemDefs;

public class BitMapHeaderPage extends BMPage {
	PageId getPageId() throws IOException {
		return getCurPage();
	}

	void setPageId(PageId pageno) throws IOException {
		setCurPage(pageno);
	}

	
 
	/**
	 * set the magic0
	 * 
	 * @param magic
	 *            magic0 will be set to be equal to magic
	 */
	void set_magic0(int magic) throws IOException {
		setPrevPage(new PageId(magic));
	}

	/**
	 * get the magic0
	 */
	int get_magic0() throws IOException {
		return getPrevPage().pid;
	};

	/**
	 * set the rootId
	 */
	void set_rootId(PageId rootID) throws IOException {
		setNextPage(rootID);
	}

	/**
	 * get the rootId
	 */
	PageId get_rootId() throws IOException {
		return getNextPage();
	}

	/**
	 * pin the page with pageno, and get the corresponding SortedPage
	 */
	public BitMapHeaderPage(PageId pageno) throws ConstructPageException {
		super();
		try {
			SystemDefs.JavabaseBM.pinPage(pageno, this, false/* Rdisk */);
		} catch (Exception e) {
			throw new ConstructPageException(e, "pinpage failed");
		}
	}

	/** associate the SortedPage instance with the Page instance */
	public BitMapHeaderPage(Page page) {

		super(page);
	}

	/**
	 * new a page, and associate the SortedPage instance with the Page instance
	 */
	public BitMapHeaderPage() throws ConstructPageException {
		super();
		try {
			Page apage = new Page();
			PageId pageId = SystemDefs.JavabaseBM.newPage(apage, 1);
			if (pageId == null)
				throw new ConstructPageException(null, "new page failed");
			this.init(pageId, apage);
			
		} catch (Exception e) {
			throw new ConstructPageException(e, "construct header page failed");
		}
	}

} // end of BitMapHeaderPage

