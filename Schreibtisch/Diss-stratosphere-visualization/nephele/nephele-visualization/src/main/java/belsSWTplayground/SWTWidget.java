package belsSWTplayground;

import org.eclipse.swt.SWT;
import org.eclipse.swt.events.DisposeEvent;
import org.eclipse.swt.events.DisposeListener;
import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Cursor;
import org.eclipse.swt.graphics.Point;
import org.eclipse.swt.layout.GridLayout;
import org.eclipse.swt.widgets.Button;
import org.eclipse.swt.widgets.Display;
import org.eclipse.swt.widgets.Label;
import org.eclipse.swt.widgets.Shell;
import org.eclipse.swt.layout.FillLayout;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.graphics.Image;
import org.eclipse.swt.graphics.Rectangle;
import org.eclipse.swt.layout.RowLayout;
import org.eclipse.swt.widgets.Event;
import org.eclipse.swt.widgets.Group;
import org.eclipse.swt.widgets.Listener;
import org.eclipse.swt.events.ControlEvent;
import org.eclipse.swt.events.ControlListener;
import org.eclipse.swt.events.SelectionAdapter;
import org.eclipse.swt.events.SelectionEvent;
import org.eclipse.swt.widgets.Composite;

public class SWTWidget {

	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    Label label = new Label(shell, SWT.SHADOW_IN | SWT.CENTER);

	    shell.setLayout(new GridLayout());

	    if ((label.getStyle() & SWT.CENTER) != 1) {
	      System.out.println("center");
	    } else {
	      System.out.println("not center");
	    }

	    shell.setSize(260, 120);
	    shell.open();

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);

	    
	     //Wird das Fenster geschlossen wird die Funktion widgetDisposed 
	     //aufgerufen.
	    shell.addDisposeListener(new DisposeListener() {
	      public void widgetDisposed(DisposeEvent e) {
	        System.out.println(e.widget + " disposed");
	      }
	    });

	    shell.setSize(260, 120);
	    shell.open();

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/


	/*public static void main(String[] args) {
		    Display display = new Display();
		    Shell shell = new Shell(display);
		    shell.setLayout(new FillLayout());
		    Button button = new Button(shell, SWT.PUSH);

		    shell.setSize(260, 120);
		    shell.open();

		    System.out.println("------------------------------");
		    System.out.println("getBounds: " + button.getBounds());
		    System.out.println("getLocation: " + button.getLocation());
		    System.out.println("getSize: " + button.getSize());

		    while (!shell.isDisposed()) {
		      if (!display.readAndDispatch()) {
		        display.sleep();
		      }
		    }
		  }*/
	
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    
	    Button button = new Button(shell, SWT.PUSH);

	    //(left, top, size witdh, size height)
	    button.setBounds(20,20,200,20);
	    
	    shell.setSize(260, 120);
	    shell.open();

	    System.out.println("------------------------------");
	    System.out.println("getBounds: " + button.getBounds());
	    System.out.println("getLocation: " + button.getLocation());
	    System.out.println("getSize: " + button.getSize());

	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    
	    Button button = new Button(shell, SWT.PUSH);
	    
	    //Analog zu button.setBounds(20,20,200,20);
	    button.setLocation(new Point(20,20));
	    button.setSize(new Point(200,20));

	    
	    shell.setSize(260, 120);
	    shell.open();

	    System.out.println("------------------------------");
	    System.out.println("getBounds: " + button.getBounds());
	    System.out.println("getLocation: " + button.getLocation());
	    System.out.println("getSize: " + button.getSize());

	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/ 
	
	  /*static Image oldImage;
	  public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display);
	    shell.setBackgroundMode(SWT.INHERIT_DEFAULT);
	    FillLayout layout1 = new FillLayout(SWT.VERTICAL);
	    layout1.marginWidth = layout1.marginHeight = 10;
	    shell.setLayout(layout1);
	    Group group = new Group(shell, SWT.NONE);
	    group.setText("Group ");
	    RowLayout layout2 = new RowLayout(SWT.VERTICAL);
	    layout2.marginWidth = layout2.marginHeight = layout2.spacing = 10;
	    group.setLayout(layout2);
	    for (int i = 0; i < 8; i++) {
	      Button button = new Button(group, SWT.RADIO);
	      button.setText("Button " + i);
	    }
	    shell.addListener(SWT.Resize, new Listener() {
	      public void handleEvent(Event event) {
	        Rectangle rect = shell.getClientArea();
	        Image newImage = new Image(display, Math.max(1, rect.width), 1);
	        GC gc = new GC(newImage);
	        gc.setForeground(display.getSystemColor(SWT.COLOR_WHITE));
	        gc.setBackground(display.getSystemColor(SWT.COLOR_BLUE));
	        gc.fillGradientRectangle(rect.x, rect.y, rect.width, 1, false);
	        gc.dispose();
	        shell.setBackgroundImage(newImage);
	        if (oldImage != null)
	          oldImage.dispose();
	        oldImage = newImage;
	      }
	    });
	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    if (oldImage != null)
	      oldImage.dispose();
	    display.dispose();
	  }*/
	
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    final Shell shell = new Shell(display);

	    shell.setLayout(new RowLayout(SWT.VERTICAL));

	    Color color = display.getSystemColor(SWT.COLOR_RED);

	    Group group = new Group(shell, SWT.NONE);
	    group.setText("SWT.INHERIT_NONE");
	    group.setBackground(color);
	    group.setBackgroundMode(SWT.INHERIT_NONE);

	    shell.pack();
	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	/*public static void main(String[] args) {
	    Display display = new Display();
	    Shell shell = new Shell(display);
	    shell.setBounds(10, 10, 200, 200);

	    Button button1 = new Button(shell, SWT.PUSH);
	    button1.setText("&Typical button");
	    button1.setBounds(10, 10, 180, 30);
	    Button button2 = new Button(shell, SWT.PUSH);
	    button2.setText("&Overidden button");
	    button2.setBounds(10, 50, 180, 30);

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch())
	        display.sleep();
	    }
	    display.dispose();
	  }*/
	
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    Shell shell = new Shell(display);

	    shell.setLayout(new GridLayout());

	    Button button = new Button(shell, SWT.PUSH | SWT.LEFT);
	    button.setText("Button");

	    System.out.println("Bounds: " + button.getBounds());
	    System.out.println("computeSize: " + button.computeSize(100, SWT.DEFAULT));
	    System.out.println("computeSize: " + button.computeSize(40, SWT.DEFAULT));
	    System.out.println("computeSize: " + button.computeSize(SWT.DEFAULT, 100));
	    System.out.println("computeSize: " + button.computeSize(SWT.DEFAULT, 20));
	    System.out.println("computeSize: " + button.computeSize(SWT.DEFAULT, 15));
	    System.out.println("computeSize: " + button.computeSize(100, 200));
	    System.out.println("computeSize: " + button.computeSize(SWT.DEFAULT, SWT.DEFAULT));

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	
	/*public static void main(String[] args) {
	    final Display display = new Display();
	    Shell shell = new Shell(display);

	    shell.setLayout(new GridLayout());

	    Button button = new Button(shell, SWT.PUSH | SWT.LEFT);
	    button.setText("Button");

	    System.out.println("toControl: " + button.toControl(100, 200));
	    System.out.println("toDisplay: " + button.toDisplay(100, 200));

	    shell.open();
	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	  }*/
	
	static int count = 0;
	public static void main(String[] args) {
	    Display display = new Display();
	    final Shell shell = new Shell(display);

	    shell.setLayout(new RowLayout());

	    final Composite composite = new Composite(shell, SWT.BORDER);
	    composite.setLayout(new RowLayout());
	    composite.setBackground(display.getSystemColor(SWT.COLOR_YELLOW));
	    composite.addControlListener(new ControlListener() {
	      public void controlMoved(ControlEvent e) {
	      }

	      public void controlResized(ControlEvent e) {
	        System.out.println("Composite resize.");
	      }
	    });    
	    
	    Button buttonAdd = new Button(shell, SWT.PUSH);
	    buttonAdd.setText("Add new button");
	    buttonAdd.addSelectionListener(new SelectionAdapter() {
	      public void widgetSelected(SelectionEvent e) {
	        Button button = new Button(composite, SWT.PUSH);
	        button.setText("Button #" + (count++));
	        composite.layout(true);
	        composite.pack();
	      }
	    });

	    shell.setSize(450, 100);
	    shell.open();

	    while (!shell.isDisposed()) {
	      if (!display.readAndDispatch()) {
	        display.sleep();
	      }
	    }
	    display.dispose();
	  }
}
