//http://www.eclipse.org/swt/snippets/
package belsSWTplayground;

import org.eclipse.swt.SWT;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.ImageData;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.graphics.Region;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Control;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.widgets.MessageBox;
import org.eclipse.swt.widgets.Monitor;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.events.MouseEvent;
import org.eclipse.swt.events.PaintEvent;
import org.eclipse.swt.events.PaintListener;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.events.SelectionListener;
import org.eclipse.swt.events.MouseListener;
import org.eclipse.swt.events.MouseMoveListener;

/*
The Shell object represents a window-either a top-level window or a dialog window. Shell Styles:
Style	Description
BORDER	Adds a border.
CLOSE	Adds a close button.
MIN	Adds a minimize button.
MAX	Adds a maximize button.
NO_TRIM	Creates a Shell that has no border and can't be moved, closed, resized, minimized, or maximized. (Useful for splash screens).
RESIZE	Adds a resizable border.
TITLE	Adds a title bar.
DIALOG_TRIM	Convenience style, equivalent to TITLE | CLOSE | BORDER.
SHELL_TRIM	Convenience style, equivalent to CLOSE | TITLE | MIN | MAX | RESIZE.
APPLICATION_MODAL	Creates a Shell that's modal to the application. Note that you should specify only one of APPLICATION_MODAL, PRIMARY_MODAL, SYSTEM_MODAL, or MODELESS; you can specify more, but only one is applied. The order of preference is SYSTEM_MODAL, APPLICATION_MODAL, PRIMARY_MODAL, then MODELESS.
PRIMARY_MODAL	Creates a primary modal Shell.
SYSTEM_MODAL	Creates a Shell that's modal system-wide.
MODELESS	Creates a modeless Shell.

Shells can be separated into two categories: top-level shells and secondary/dialog shells.
Shells that do not have a parent are top-level shells.
All dialog shells have a parent.
Use the following constructors to create top-level shells:
Shell()
Shell(int style)
Shell(Display display)
Shell(Display display, int style)
Dialog shells can be created using the following constructors:
Shell(Shell parent)
Shell(Shell parent, int style)
 */
public class SWTShell {
	
	//Öffnet die Shell minimiert, das Fenster ist Maximiert
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    //minimiert
	    shell.setMinimized(true);
	    //in maximaler größe
	    shell.setMaximized(true);
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	//Öffnet das Fenster zentriert in der Mitte des Monitors
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    shell.setSize(200, 200);
	    
	    Monitor primary = display.getPrimaryMonitor();
	    Rectangle bounds = primary.getBounds();
	    Rectangle rect = shell.getBounds();
	    
	    int x = bounds.x + (bounds.width - rect.width) / 2;
	    int y = bounds.y + (bounds.height - rect.height) / 2;
	    
	    shell.setLocation(x, y);
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	/*public static void main(String[] args) {
	    Display display = new Display();

	    Image small = new Image(display, 16, 16);
	    GC gc = new GC(small);
	    gc.setBackground(display.getSystemColor(SWT.COLOR_RED));
	    gc.fillArc(0, 0, 16, 16, 45, 270);
	    gc.dispose();

	    Image large = new Image(display, 32, 32);
	    gc = new GC(large);
	    gc.setBackground(display.getSystemColor(SWT.COLOR_RED));
	    gc.fillArc(0, 0, 32, 32, 45, 270);
	    gc.dispose();

	    //Provide different resolutions for icons to get high quality rendering
	    //wherever the OS needs large icons. For example, the ALT+TAB window on
	    //certain systems uses a larger icon.
	    Shell shell = new Shell(display);
	    shell.setText("Small and Large icons");
	    shell.setImages(new Image[] { small, large });

	    //No large icon: the OS will scale up the small icon when it needs a large one.
	    Shell shell2 = new Shell(display);
	    shell2.setText("Small icon");
	    shell2.setImage(small);

	    shell.open();
	    shell2.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    small.dispose();
	    large.dispose();
	    display.dispose();
	  }*/
	
	//Magic Window =D Formlos
	/*
	static int[] circle(int r, int offsetX, int offsetY) {
	    int[] polygon = new int[8 * r + 4];
	    // x^2 + y^2 = r^2
	    for (int i = 0; i < 2 * r + 1; i++) {
	      int x = i - r;
	      int y = (int) Math.sqrt(r * r - x * x);
	      polygon[2 * i] = offsetX + x;
	      polygon[2 * i + 1] = offsetY + y;
	      polygon[8 * r - 2 * i - 2] = offsetX + x;
	      polygon[8 * r - 2 * i - 1] = offsetY - y;
	    }
	    return polygon;
	  }

	  public static void main(String[] args) {
	    final Display display = new Display();
	    // Shell must be created with style SWT.NO_TRIM
	    final Shell shell = new Shell(display, SWT.NO_TRIM | SWT.ON_TOP);
	    shell.setBackground(display.getSystemColor(SWT.COLOR_RED));
	    // define a region that looks like a key hole
	    Region region = new Region();
	    region.add(circle(67, 67, 67));
	    region.subtract(circle(20, 67, 50));
	    region.subtract(new int[] { 67, 50, 55, 105, 79, 105 });
	    // define the shape of the shell using setRegion
	    shell.setRegion(region);
	    Rectangle size = region.getBounds();
	    shell.setSize(size.width, size.height);
	    // add ability to move shell around
	    Listener l = new Listener() {
	      Point origin;

	      public void handleEvent(Event e) {
	        switch (e.type) {
	        case SWT.MouseDown:
	          origin = new Point(e.x, e.y);
	          break;
	        case SWT.MouseUp:
	          origin = null;
	          break;
	        case SWT.MouseMove:
	          if (origin != null) {
	            Point p = display.map(shell, null, e.x, e.y);
	            shell.setLocation(p.x - origin.x, p.y - origin.y);
	          }
	          break;
	        }
	      }
	    };
	    shell.addListener(SWT.MouseDown, l);
	    shell.addListener(SWT.MouseUp, l);
	    shell.addListener(SWT.MouseMove, l);
	    // add ability to close shell
	    Button b = new Button(shell, SWT.PUSH);
	    b.setBackground(shell.getBackground());
	    b.setText("close");
	    b.pack();
	    b.setLocation(10, 68);
	    b.addListener(SWT.Selection, new Listener() {
	      public void handleEvent(Event e) {
	        shell.close();
	      }
	    });
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    region.dispose();
	    display.dispose();
	  }*/
	
	//Create a non-rectangular shell to simulate transparency
	/*public static void main(String[] args) {
	    Display display = new Display();
	    final Image image = display.getSystemImage(SWT.ICON_WARNING);
	    // Shell must be created with style SWT.NO_TRIM
	    final Shell shell = new Shell(display, SWT.NO_TRIM | SWT.ON_TOP);
	    shell.setBackground(display.getSystemColor(SWT.COLOR_RED));
	    // define a region
	    Region region = new Region();
	    Rectangle pixel = new Rectangle(0, 0, 1, 1);
	    for (int y = 0; y < 200; y += 2) {
	      for (int x = 0; x < 200; x += 2) {
	        pixel.x = x;
	        pixel.y = y;
	        region.add(pixel);
	      }
	    }
	    // define the shape of the shell using setRegion
	    shell.setRegion(region);
	    Rectangle size = region.getBounds();
	    shell.setSize(size.width, size.height);
	    shell.addPaintListener(new PaintListener() {
	      public void paintControl(PaintEvent e) {
	        Rectangle bounds = image.getBounds();
	        Point size = shell.getSize();
	        e.gc.drawImage(image, 0, 0, bounds.width, bounds.height, 10, 10, size.x - 20, size.y - 20);
	      }
	    });
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    region.dispose();
	    display.dispose();
	  }*/
	
	
	//Create non-rectangular shell from an image with transparency
	//Funktioniert zumindestens unter Linux nicht
	 /*public static void main(String[] args) {
		    final Display display = new Display();
		    final Image image = display.getSystemImage(SWT.ICON_INFORMATION);
		    final Shell shell = new Shell(display, SWT.NO_TRIM);
		    Region region = new Region();
		    final ImageData imageData = image.getImageData();
		    if (imageData.alphaData != null) {
		      Rectangle pixel = new Rectangle(0, 0, 1, 1);
		      for (int y = 0; y < imageData.height; y++) {
		        for (int x = 0; x < imageData.width; x++) {
		          if (imageData.getAlpha(x, y) == 255) {
		            pixel.x = imageData.x + x;
		            pixel.y = imageData.y + y;
		            region.add(pixel);
		          }
		        }
		      }
		    } else {
		      ImageData mask = imageData.getTransparencyMask();
		      Rectangle pixel = new Rectangle(0, 0, 1, 1);
		      for (int y = 0; y < mask.height; y++) {
		        for (int x = 0; x < mask.width; x++) {
		          if (mask.getPixel(x, y) != 0) {
		            pixel.x = imageData.x + x;
		            pixel.y = imageData.y + y;
		            region.add(pixel);
		          }
		        }
		      }
		    }
		    shell.setRegion(region);

		    Listener l = new Listener() {
		      int startX, startY;

		      public void handleEvent(Event e) {
		        if (e.type == SWT.KeyDown && e.character == SWT.ESC) {
		          shell.dispose();
		        }
		        if (e.type == SWT.MouseDown && e.button == 1) {
		          startX = e.x;
		          startY = e.y;
		        }
		        if (e.type == SWT.MouseMove && (e.stateMask & SWT.BUTTON1) != 0) {
		          Point p = shell.toDisplay(e.x, e.y);
		          p.x -= startX;
		          p.y -= startY;
		          shell.setLocation(p);
		        }
		        if (e.type == SWT.Paint) {
		          e.gc.drawImage(image, imageData.x, imageData.y);
		        }
		      }
		    };
		    shell.addListener(SWT.KeyDown, l);
		    shell.addListener(SWT.MouseDown, l);
		    shell.addListener(SWT.MouseMove, l);
		    shell.addListener(SWT.Paint, l);

		    shell.setSize(imageData.x + imageData.width, imageData.y + imageData.height);
		    shell.open();
		    while (!shell.isDisposed()) {
		      if (!display.readAndDispatch())
		        display.sleep();
		    }
		    region.dispose();
		    image.dispose();
		    display.dispose();
		  }*/
	
	
	//Allow escape to close a shell
	/*public static void main(String[] args) {
	    Display display = new Display();
	    final Shell shell = new Shell(display);
	    //Macht dass das Fenster größer ist, z B so groß wie der Button unten
	    //shell.setLayout(new FillLayout());
	    shell.addListener(SWT.Traverse, new Listener() {
	      public void handleEvent(Event event) {
	        switch (event.detail) {
	        case SWT.TRAVERSE_ESCAPE:
	          shell.close();
	          event.detail = SWT.TRAVERSE_NONE;
	          event.doit = false;
	          break;
	        }
	      }
	    });
	    //Button button = new Button(shell, SWT.PUSH);
	    //button.setText("A Button (that doesn't process Escape)");
	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	
	//Prevent a shell from closing (prompt the user)
	/*
	public static void main(String[] args) {
	    Display display = new Display();
	    final Shell shell = new Shell(display);
	    shell.addListener(SWT.Close, new Listener() {
	      public void handleEvent(Event event) {
	        int style = SWT.APPLICATION_MODAL | SWT.YES | SWT.NO;
	        MessageBox messageBox = new MessageBox (shell, style);
	        messageBox.setText ("Information");
	        messageBox.setMessage ("Close the shell?");
	        event.doit = messageBox.open () == SWT.YES;

	      }
	    });
	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	//Sollte ein Viereck malen, funktioniert aber leider nicht
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    shell.open();
	    GC gc = new GC(shell);
	    gc.setForeground(display.getSystemColor(SWT.COLOR_BLUE));
	    gc.setLineWidth(4);
	    gc.drawRectangle(20, 20, 100, 100);
	    gc.dispose();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	
	//Getting Controls on a shell
	/*public static void main(String[] args) {
	    Display display = new Display();
	    final Shell shell = new Shell(display);
	    shell.setLayout(new FillLayout());
	    
	    for (int i = 0; i < 20; i++) {
	      Button button = new Button(shell, SWT.TOGGLE);
	      button.setText("B" + i);
	    }
	    Control[] children = shell.getChildren();
	    for (int i = 0; i < children.length; i++) {
	      Control child = children[i];
	      System.out.println(child);
	    }

	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	//Set default Button for Shell
	/*
	public static void main(String[] args) {
	    final Display display = new Display();
	    Shell shell = new Shell(display);

	    shell.setLayout(new RowLayout());

	    final String[] ratings = new String[] { "A!", "B", "C" };
	    final Button[] radios = new Button[ratings.length];
	    for (int i = 0; i < ratings.length; i++) {
	      radios[i] = new Button(shell, SWT.RADIO);
	      radios[i].setText(ratings[i]);
	    }

	    Button cancelButton = new Button(shell, SWT.PUSH);
	    cancelButton.setText("Canel");

	    Button rateButton = new Button(shell, SWT.PUSH);
	    rateButton.setText("OK");
	    rateButton.addSelectionListener(new SelectionListener() {
	      public void widgetSelected(SelectionEvent e) {
	        for (int i = 0; i < radios.length; i++) {
	          if (radios[i].getSelection()) {
	            System.out.println(ratings[i]);
	          }
	        }
	      }

	      public void widgetDefaultSelected(SelectionEvent e) {
	        for (int i = 0; i < radios.length; i++) {
	          if (radios[i].getSelection()) {
	            System.out.println(ratings[i]);
	          }
	        }
	      }
	    });

	    shell.setDefaultButton(rateButton);

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	
	//Get Default Button
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    Shell shell = new Shell(display);

	    shell.setLayout(new RowLayout());

	    Button cancelButton = new Button(shell, SWT.PUSH);
	    cancelButton.setText("Canel");

	    Button rateButton = new Button(shell, SWT.PUSH);
	    rateButton.setText("OK");
	    shell.setDefaultButton(rateButton);
	    System.out.println(shell.getDefaultButton());

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	//Shell with SWT.SHELL_TRIM|SWT.TOOL style
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    Shell shell = new Shell(display, SWT.SHELL_TRIM | SWT.TOOL);

	    shell.setLayout(new GridLayout());

	    Button button = new Button(shell, SWT.PUSH | SWT.LEFT);
	    button.setText("Button");

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	
	//Ring Shell
	/*
	static int[] createCircle(int radius, int centerX, int centerY) {
	    int[] points = new int[360 * 2];
	    for(int i=0; i<360; i++) {
	      points[i*2] = centerX + (int)(radius * Math.cos(Math.toRadians(i)));
	      points[i*2+1] = centerY + (int)(radius * Math.sin(Math.toRadians(i)));
	    }
	    return points;
	  }
	  
	  static Point originalPosition = null;
	  
	  public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display, SWT.NO_TRIM | SWT.ON_TOP);
	    shell.setBackground(display.getSystemColor(SWT.COLOR_DARK_MAGENTA));
	    
	    Region region = new Region();
	    region.add(createCircle(100, 100, 100));
	    region.subtract(createCircle(50, 100, 100));
	    shell.setRegion(region);
	    
	    // Make the shell movable by using the mouse pointer. 
	    shell.addMouseListener(new MouseListener() {
	      public void mouseDoubleClick(MouseEvent e) {
	        shell.dispose(); // Double click to dispose the shell.
	      }

	      public void mouseDown(MouseEvent e) {
	        originalPosition = new Point(e.x, e.y);
	      }

	      public void mouseUp(MouseEvent e) {
	        originalPosition = null;
	      }
	    });
	    
	    shell.addMouseMoveListener(new MouseMoveListener() {
	      public void mouseMove(MouseEvent e) {
	        if(originalPosition == null)
	          return;
	        Point point = display.map(shell, null, e.x, e.y);
	        shell.setLocation(point.x - originalPosition.x, point.y - originalPosition.y);
	        System.out.println("Moved from: " + originalPosition + " to " + point);
	      }
	    });
	    
	    Rectangle regionBounds = region.getBounds();
	    shell.setSize(regionBounds.width, regionBounds.height);
	    shell.open();

	    // Set up the event loop.
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        // If no more entries in event queue
	        display.sleep();
	      }
	    }
	    display.dispose();
	    region.dispose();
	  }*/
	
	
	//Shell without Title bar
	public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display, SWT.NO_TRIM | SWT.ON_TOP);

	    shell.open();

	    // Set up the event loop.
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        // If no more entries in event queue
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }
	
	//Mehr zB unter http://www.java2s.com/Tutorial/Java/0280__SWT/0080__Shell.htm

}
