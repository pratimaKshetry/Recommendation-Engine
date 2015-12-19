/******************
Original Author: Pratima Kshetry
*/

package pkProductProfile;

public class ProductProfile {
	public String id;
	public String title;
	public String avgRating;	
	public String similarProductIDs;
	public String toString()
	{
		String str= this.title+"||"+this.avgRating+"||"+this.similarProductIDs;
		return str;
	}
}
