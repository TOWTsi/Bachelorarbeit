package belsSWTplayground;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Shell;

public class SWTDisplay{
	
	//Zeigt auf der Konsole wie groß das Fenster des Bildschirmes ist
  /*public static void main(String[] args) {
    Display display = new Display();
    System.out.println("Display Bounds=" + display.getBounds() + " Display ClientArea="
        + display.getClientArea());
    display.dispose();
  }*/
  	
	/*static Display display = new Display();
	  public static void main(String[] args) {
		//Gibt an wie die Buttons alignet werden
	    System.out.println(display.getDismissalAlignment() == SWT.LEFT);
	    System.out.println(display.getDismissalAlignment() == SWT.RIGHT);
	    //Gibt die maximale Zeit zwischen zwei Klicks an, damit diese als 
	    //Doppelklick zählen (In millisekunden)
	    System.out.println(display.getDoubleClickTime());
	    //Maximale Tiefe von Icons (gibt 24 aus, aber Bedeutung?)
	    System.out.println(display.getIconDepth());
	    //Get all the monitors attached to the device, call Display.getMonitors()
	    System.out.println(display.getPrimaryMonitor().getClientArea());
	    //The returned color should not be freed because it was allocated and managed by the system
	    //Color col = display.getSystemColor(SWT.COLOR_BLUE);
	  }*/
	
	//The readAndDispatch reads events from the operating 
	//system's event queue and then dispatches them appropriately.
	public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    shell.setText("Hello, world!");
	    shell.open();

	    while(! shell.isDisposed()) { // Event loop.
	        if(! display.readAndDispatch())
	          display.sleep();
	    }
	    display.dispose();
	  }
	
}
