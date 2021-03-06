package eu.stratosphere.sopremo.io;

import it.unimi.dsi.fastutil.Stack;
import it.unimi.dsi.fastutil.chars.Char2ObjectMap;
import it.unimi.dsi.fastutil.chars.Char2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.sopremo.type.AbstractJsonNode;
import eu.stratosphere.sopremo.type.ArrayNode;
import eu.stratosphere.sopremo.type.BigIntegerNode;
import eu.stratosphere.sopremo.type.BooleanNode;
import eu.stratosphere.sopremo.type.DecimalNode;
import eu.stratosphere.sopremo.type.IArrayNode;
import eu.stratosphere.sopremo.type.IJsonNode;
import eu.stratosphere.sopremo.type.IObjectNode;
import eu.stratosphere.sopremo.type.IntNode;
import eu.stratosphere.sopremo.type.LongNode;
import eu.stratosphere.sopremo.type.NullNode;
import eu.stratosphere.sopremo.type.ObjectNode;
import eu.stratosphere.sopremo.type.TextNode;

/**
 * Creates a JsonNode-representation of the provided data. The input-data must be provided in a valid json-format.
 */
public class JsonParser {
	// TODO: rewrite to a state-based parser, that supports escaping

	private final BufferedReader reader;

	private final Stack<IJsonNode> state = new ObjectArrayList<IJsonNode>();

	ContainerNode root = new ContainerNode();

	private boolean insideString = false;

	private boolean wasString = false;

	private boolean isArray = false;

	private boolean reachedEnd = false;

	private final Char2ObjectMap<CharacterHandler> handler = new Char2ObjectOpenHashMap<CharacterHandler>(
		new char[] { '[', ']', '{', '}', ':', '\"', ',', ' ' },
		new CharacterHandler[] { new OpenArrayHandler(), new CloseHandler(), new OpenObjectHandler(),
			new CloseHandler(), new KeyValueSeperatorHandler(), new StringHandler(),
			new CommaHandler(), new WhiteSpaceHandler() });

	/**
	 * Initializes a JsonParser that reads the input-data from a {@link FSDataInputStream}.
	 * 
	 * @param stream
	 *        the stream that provides the data
	 */
	public JsonParser(final InputStream stream) {
		this(new InputStreamReader(stream, Charset.forName("utf-8")));
	}

	/**
	 * Initializes a JsonParser that reads the input-data from a {@link Reader}.
	 * 
	 * @param inputStreamReader
	 *        the reader that provides the data
	 */
	public JsonParser(final Reader inputStreamReader) {
		this.reader = new BufferedReader(inputStreamReader);
		this.handler.defaultReturnValue(new DefaultHandler());
	}

	/**
	 * Initializes a JsonParser that reads the input-data from an {@link URL}.
	 * 
	 * @param url
	 *        the url that provides the data
	 * @throws IOException
	 */
	public JsonParser(final URL url) throws IOException {
		this(new BufferedReader(new InputStreamReader(url.openStream())));
	}

	/**
	 * Initializes a JsonParser directly with the input-data.
	 * 
	 * @param value
	 *        the data that should be parsed
	 */
	public JsonParser(final String value) {
		this(new BufferedReader(new StringReader(value)));
	}

	public IJsonNode readValueAsTree() throws IOException {

		this.state.push(this.root);

		final StringBuilder sb = new StringBuilder();
		int nextChar = this.reader.read();

		if (this.reachedEnd || nextChar == -1)
			throw new NoSuchElementException("Reached end of json document!");

		while ((this.state.top() != this.root || nextChar != ',') && nextChar != -1) {
			final char character = (char) nextChar;
			if (this.insideString && character != '\"')
				sb.append(character);
			else
				this.handler.get(character).handleCharacter(sb, character);
			nextChar = this.reader.read();

		}
		; // while ((this.state.top() != root || nextChar != ',') && (nextChar) != -1);

		if (sb.length() != 0) {
			// if(root == state.top())
			this.root.addValue(JsonParser.parsePrimitive(sb.toString()));
			if (!this.isArray)
				this.reachedEnd = true;
			// else {
			// return JsonParser.parsePrimitive(sb.toString());
			// }
			sb.setLength(0);

		}

		// if(isArray){
		return this.root.remove(0);
		// } else {
		// // ArrayNode result = (ArrayNode) root.build();
		// // root.remove(0);
		// return root.remove(0);
		// }

	}

	/**
	 * Returns either the parsing of the input-data is finished or not.
	 * 
	 * @return the parsing is completed
	 */
	public boolean checkEnd() {
		return this.reachedEnd;
	}

	/**
	 * Closes the connection to the data source.
	 * 
	 * @throws IOException
	 */
	public void close() throws IOException {
		this.reader.close();
	}

	private static IJsonNode parsePrimitive(final String value) {
		if (value.equals("null"))
			return NullNode.getInstance();
		if (value.equals("true"))
			return BooleanNode.TRUE;
		if (value.equals("false"))
			return BooleanNode.FALSE;
		if (value.matches("^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$")) {
			final BigDecimal bigDec = new BigDecimal(value);
			if (bigDec.scale() == 0) {
				final BigInteger bigInt = bigDec.unscaledValue();
				if (bigInt.bitLength() <= 31)
					return IntNode.valueOf(bigInt.intValue());
				if (bigInt.bitLength() <= 63)
					return LongNode.valueOf(bigInt.longValue());
				return BigIntegerNode.valueOf(bigInt);
			}
			return DecimalNode.valueOf(bigDec);
		}

		return TextNode.valueOf(value);
	}

	/**
	 * Interface for all character handler. These handlers are responsible for processing special characters in the
	 * json-format.
	 */
	private interface CharacterHandler {
		/**
		 * Implementation of this method specifies how this handler should behave.
		 * 
		 * @throws JsonParseException
		 */
		public void handleCharacter(StringBuilder sb, char character) throws JsonParseException;
	}

	/**
	 * {@link CharacterHandler} that handles the begin of an array.
	 */
	private class OpenArrayHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) {
			if (JsonParser.this.root == JsonParser.this.state.top() && !JsonParser.this.isArray)
				JsonParser.this.isArray = true;
			else
				JsonParser.this.state.push(new ContainerNode());
		}

	}

	/**
	 * {@link CharacterHandler} that handles the begin of an object.
	 */
	private class OpenObjectHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) {
			final ContainerNode node = new ContainerNode();
			JsonParser.this.state.push(node);

		}

	}

	/**
	 * {@link CharacterHandler} that handles the end of arrays and objects.
	 */
	private class CloseHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) throws JsonParseException {
			ContainerNode node;
			if (JsonParser.this.state.top() != JsonParser.this.root) {
				node = (ContainerNode) JsonParser.this.state.pop();
				if (JsonParser.this.state.top() == JsonParser.this.root && !JsonParser.this.isArray)
					JsonParser.this.reachedEnd = true;
			} else {
				node = (ContainerNode) JsonParser.this.state.top();
				JsonParser.this.reachedEnd = true;
			}

			if (sb.length() != 0) {
				if (!JsonParser.this.wasString)
					node.addValue(JsonParser.parsePrimitive(sb.toString()));
				else {
					node.addValue(TextNode.valueOf(sb.toString()));
					JsonParser.this.wasString = false;
				}
				sb.setLength(0);
			}

			if (node != JsonParser.this.root)
				((ContainerNode) JsonParser.this.state.top()).addValue(node.build());
		}
	}

	/**
	 * {@link CharacterHandler} that handles begin and end of strings.
	 */
	private class StringHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) {
			if (sb.length() == 0)
				JsonParser.this.insideString = true;
			else if (!sb.toString().endsWith("\\")) {
				JsonParser.this.insideString = false;
				JsonParser.this.wasString = true;
			} else
				sb.append(character);
		}
	}

	/**
	 * {@link CharacterHandler} that handles commas.
	 */
	private class CommaHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) {
			if (sb.length() != 0) {
				final ContainerNode node = (ContainerNode) JsonParser.this.state.top();
				if (!JsonParser.this.wasString)
					node.addValue(JsonParser.parsePrimitive(sb.toString()));
				else {
					node.addValue(TextNode.valueOf(sb.toString()));
					JsonParser.this.wasString = false;
				}
				sb.setLength(0);
			}
		}
	}

	/**
	 * {@link CharacterHandler} that handles a whitespace.
	 */
	private class WhiteSpaceHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) {
			if (JsonParser.this.insideString)
				sb.append(character);
		}

	}

	/**
	 * {@link CharacterHandler} that handles the separation of key-value-pairs.
	 */
	private class KeyValueSeperatorHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) throws JsonParseException {
			if (sb.length() != 0) {
				final ContainerNode node = (ContainerNode) JsonParser.this.state.top();
				node.addKey(sb);
				JsonParser.this.wasString = false;
				sb.setLength(0);
			}
		}

	}

	/**
	 * {@link CharacterHandler} that handles unknown characters.
	 */
	private class DefaultHandler implements CharacterHandler {

		@Override
		public void handleCharacter(final StringBuilder sb, final char character) throws JsonParseException {

			if (Character.isWhitespace(character))
				JsonParser.this.handler.get(' ').handleCharacter(sb, character);
			else
				sb.append(character);

		}
	}

	/**
	 * {@link JsonNode} that represents an unfinished complex-node in the parsing-process.
	 */
	private class ContainerNode extends AbstractJsonNode {

		private static final long serialVersionUID = -7285733826083281420L;

		private final List<String> keys = new ArrayList<String>();

		private final List<IJsonNode> values = new ArrayList<IJsonNode>();

		/**
		 * Adds the data contained in the given {@link StringBuilder} as a key.
		 * 
		 * @param sb
		 *        the string builder that contains the data
		 */
		public void addKey(final StringBuilder sb) {
			this.keys.add(sb.toString());
		}

		public void addValue(final IJsonNode node) {
			this.values.add(node);
		}

		public IJsonNode remove(final int index) throws JsonParseException {
			if (this.keys.isEmpty())
				return this.values.remove(index);
			throw new JsonParseException();
		}

		public IJsonNode build() throws JsonParseException {
			IJsonNode node;

			if (this.keys.size() == 0) {
				// this ContainerNode represents an ArrayNode
				node = new ArrayNode();
				for (final IJsonNode value : this.values)
					((IArrayNode) node).add(value);

			} else {
				// this ContainerNode represents an ObjectNode

				if (this.keys.size() != this.values.size())
					throw new JsonParseException();

				node = new ObjectNode();
				for (int i = 0; i < this.keys.size(); i++)
					((IObjectNode) node).put(this.keys.get(i), this.values.get(i));
			}

			return node;
		}

		@Override
		public Type getType() {
			return null;
		}

		@Override
		public void read(final DataInput in) throws IOException {
		}

		@Override
		public void write(final DataOutput out) throws IOException {
		}

		@Override
		public void appendAsString(final Appendable sb) throws IOException {
		}

		@Override
		public int compareTo(final Key arg0) {
			return 0;
		}

		@Override
		public int compareToSameType(final IJsonNode other) {
			return 0;
		}

		@Override
		public void clear() {
		}

		@Override
		public int getMaxNormalizedKeyLen() {
			return 0;
		}

		@Override
		public void copyNormalizedKey(final byte[] target, final int offset, final int len) {
		}

		@Override
		public void copyValueFrom(final IJsonNode otherNode) {

		}

		/*
		 * (non-Javadoc)
		 * @see eu.stratosphere.sopremo.type.AbstractJsonNode#hashCode()
		 */
		@Override
		public int hashCode() {
			return 0;
		}
	}
}
