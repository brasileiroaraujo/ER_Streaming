package DataStructures;

import java.text.Normalizer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.github.andrewoma.dexx.collection.ArrayList;

import tokens.KeywordGenerator;
import tokens.KeywordGeneratorImpl;

public class TokenExtractor {
	
	public static Set<String> generateTokens(String string) {
//		Pattern p = Pattern.compile("[^a-zA-Z\\s0-9]");
//		Matcher m = p.matcher("");
//		m.reset(string);
//		String standardString = m.replaceAll("");
		
		//avoid to send empty tokens
		if (string.trim().isEmpty()) {
			return new HashSet<String>();
		}
		
		//remove acentos
		String standardString = Normalizer.normalize(string, Normalizer.Form.NFD).replaceAll("[^\\p{ASCII}]", "").toLowerCase();
		
		String[] portugueseStopWords = {" ", "a", "agora", "ainda", "alguem", "algum", "alguma", "algumas", "alguns", "ampla", "amplas", "amplo", "amplos", "ante", "antes", "ao", "aos", "apos", "aquela", "aquelas", "aquele", "aqueles", "aquilo", "as", "ate", "atraves", "cada", "coisa", "coisas", "com", "como", "contra", "contudo", "da", "daquele", "daqueles", "das", "de", "dela", "delas", "dele", "deles", "depois", "dessa", "dessas", "desse", "desses", "desta", "destas", "deste", "deste", "destes", "deve", "devem", "devendo", "dever", "devera", "deverao", "deveria", "deveriam", "devia", "deviam", "disse", "disso", "disto", "dito", "diz", "dizem", "do", "dos", "e", "e", "ela", "elas", "ele", "eles", "em", "enquanto", "entre", "era", "essa", "essas", "esse", "esses", "esta", "esta", "estamos", "estao", "estas", "estava", "estavam", "estavamos", "este", "estes", "estou", "eu", "fazendo", "fazer", "feita", "feitas", "feito", "feitos", "foi", "for", "foram", "fosse", "fossem", "grande", "grandes", "ha", "isso", "isto", "ja", "la", "la", "lhe", "lhes", "lo", "mas", "me", "mesma", "mesmas", "mesmo", "mesmos", "meu", "meus", "minha", "minhas", "muita", "muitas", "muito", "muitos", "na", "nao", "nas", "nem", "nenhum", "nessa", "nessas", "nesta", "nestas", "ninguem", "no", "nos", "nos", "nossa", "nossas", "nosso", "nossos", "num", "numa", "nunca", "o", "os", "ou", "outra", "outras", "outro", "outros", "para", "pela", "pelas", "pelo", "pelos", "pequena", "pequenas", "pequeno", "pequenos", "per", "perante", "pode", "pude", "podendo", "poder", "poderia", "poderiam", "podia", "podiam", "pois", "por", "porem", "porque", "posso", "pouca", "poucas", "pouco", "poucos", "primeiro", "primeiros", "propria", "proprias", "proprio", "proprios", "quais", "qual", "quando", "quanto", "quantos", "que", "quem", "sao", "se", "seja", "sejam", "sem", "sempre", "sendo", "sera", "serao", "seu", "seus", "si", "sido", "so", "sob", "sobre", "sua", "suas", "talvez", "tambem", "tampouco", "te", "tem", "tendo", "tenha", "ter", "teu", "teus", "ti", "tido", "tinha", "tinham", "toda", "todas", "todavia", "todo", "todos", "tu", "tua", "tuas", "tudo", "ultima", "ultimas", "ultimo", "ultimos", "um", "uma", "umas", "uns", "vendo", "ver", "vez", "vindo", "vir", "vos", "vos"};
		List<String> listStopWords = Arrays.asList(portugueseStopWords);
		
		
		Set<String> keyWords = new HashSet(Arrays.asList(gr.demokritos.iit.jinsect.utils.splitToWords(standardString)));
		
//		KeywordGenerator kw = new KeywordGeneratorImpl();
//		Set<String> keyWords = kw.generateKeyWords(string);
		keyWords.removeAll(listStopWords);
		return keyWords;
	}
	
	public static void main(String[] args) {
		String s = "Você vai fazer alguma coisa aí? ou não? Vai Corinthians! Foda-se Grêmio!";
		
		Set<String> keyWords = generateTokens(s);
		
		System.out.println(keyWords);
	}

}
