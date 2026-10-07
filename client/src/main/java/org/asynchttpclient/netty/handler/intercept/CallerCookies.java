/*
 *    Copyright (c) 2026 AsyncHttpClient Project. All rights reserved.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */
package org.asynchttpclient.netty.handler.intercept;

import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.cookie.ClientCookieDecoder;
import io.netty.handler.codec.http.cookie.Cookie;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.cookie.CookieStore;
import org.asynchttpclient.uri.Uri;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static io.netty.handler.codec.http.HttpHeaderNames.COOKIE;
import static io.netty.handler.codec.http.HttpHeaderNames.SET_COOKIE;

/**
 * Tells the cookies a caller put on a request from the ones the cookie store added, for the request a redirect
 * or an authentication retry builds from it. The store's may be older than what the response just set.
 */
final class CallerCookies {

    private CallerCookies() {
    }

    /**
     * The request's cookie objects minus the ones the store put there: those this response set, rotated or
     * deleted, and those the store still holds with the same value. A caller's cookie sharing only a name with
     * a stored one stays the caller's. A Cookie header the caller set is not among these; see
     * {@link #headerCookies}.
     */
    private static List<Cookie> of(Request request, CookieStore cookieStore, Set<String> setByResponse) {
        List<Cookie> cookies = request.getCookies();
        if (cookies.isEmpty()) {
            return cookies;
        }
        List<Cookie> stored = cookieStore.get(request.getUri());
        List<Cookie> callers = new ArrayList<>(cookies.size());
        for (Cookie cookie : cookies) {
            if (!setByResponse.contains(cookie.name().toLowerCase(Locale.ROOT)) && !holdsSameValue(stored, cookie)) {
                callers.add(cookie);
            }
        }
        return callers;
    }

    private static Set<String> replacedNames(HttpResponse response, CookieStore cookieStore, Uri next,
                                             ClientCookieDecoder cookieDecoder) {
        // Only what the store took counts: a Set-Cookie it refused, or one for another path, leaves the
        // caller's cookie of that name in place. Names folded as the store keys them, so a SID set here also
        // replaces a request's sid.
        Set<String> setByResponse = new HashSet<>();
        List<Cookie> nextCookies = null;
        for (String header : response.headers().getAll(SET_COOKIE)) {
            Cookie cookie;
            try {
                cookie = cookieDecoder.decode(header);
            } catch (IllegalArgumentException e) {
                // Interceptors dropped it too, so the store still holds the old value and holdsSameValue decides.
                continue;
            }
            if (cookie == null) {
                continue;
            }
            if (cookie.maxAge() != Cookie.UNDEFINED_MAX_AGE && cookie.maxAge() <= 0) {
                setByResponse.add(cookie.name().toLowerCase(Locale.ROOT));
                continue;
            }
            if (nextCookies == null) {
                nextCookies = cookieStore.get(next);
            }
            if (holdsSameValue(nextCookies, cookie)) {
                setByResponse.add(cookie.name().toLowerCase(Locale.ROOT));
            }
        }
        return setByResponse;
    }

    /**
     * For a retry of the same request: the caller's cookies, then the store's current ones.
     */
    static void refresh(RequestBuilder retry, Request request, HttpResponse response, CookieStore cookieStore,
                        ClientCookieDecoder cookieDecoder) {
        refresh(retry, request, response, cookieStore, request.getUri(), cookieDecoder);
    }

    static void refresh(RequestBuilder retry, Request request, HttpResponse response, CookieStore cookieStore,
                        Uri next, ClientCookieDecoder cookieDecoder) {
        Set<String> setByResponse = replacedNames(response, cookieStore, next, cookieDecoder);
        retry.setCookies(of(request, cookieStore, setByResponse));
        retry.setHeader(COOKIE, headerCookies(request.getHeaders().getAll(COOKIE), setByResponse));
        for (Cookie cookie : cookieStore.get(next)) {
            retry.addCookieIfUnset(cookie);
        }
    }

    /**
     * The caller's Cookie header values minus the pairs this response replaced, kept as text: a lenient decode
     * followed by strict encoding can reject a value that was sent on the first attempt.
     * <p>
     * Every ';' ends a pair, inside quotes too: RFC 6265 does not allow one in a cookie value, and
     * {@code NettyRequestFactory#mergeCookies} splits the same way when it decides which names the caller set.
     * Honouring quotes here would let a stray '"' hide a replaced pair in its neighbour's value, to be resent
     * stale while the merge still counts its name as the caller's and drops the store's new value. Only the
     * replaced pairs are cut out, so the rest keeps its bytes and a header that loses none is returned as is.
     */
    private static List<String> headerCookies(List<String> headers, Set<String> setByResponse) {
        if (setByResponse.isEmpty() || headers.isEmpty()) {
            return headers;
        }
        List<String> remaining = new ArrayList<>(headers.size());
        for (String header : headers) {
            StringBuilder kept = new StringBuilder(header.length());
            boolean removed = false;
            int start = 0;
            while (start <= header.length()) {
                int end = header.indexOf(';', start);
                if (end < 0) {
                    end = header.length();
                }
                int nameEnd = start;
                while (nameEnd < end && header.charAt(nameEnd) != '=') {
                    nameEnd++;
                }
                String name = header.substring(start, nameEnd).trim();
                if (!name.isEmpty() && setByResponse.contains(name.toLowerCase(Locale.ROOT))) {
                    removed = true;
                } else {
                    if (kept.length() > 0) {
                        kept.append(';');
                    }
                    kept.append(header, start, end);
                }
                start = end + 1;
            }
            if (!removed) {
                remaining.add(header);
                continue;
            }
            // Cutting the first or last pair leaves the whitespace or ';' that separated it at that end.
            int from = 0;
            int to = kept.length();
            while (from < to && (kept.charAt(from) == ';' || kept.charAt(from) <= ' ')) {
                from++;
            }
            while (to > from && (kept.charAt(to - 1) == ';' || kept.charAt(to - 1) <= ' ')) {
                to--;
            }
            if (from < to) {
                remaining.add(kept.substring(from, to));
            }
        }
        return remaining;
    }

    private static boolean holdsSameValue(List<Cookie> stored, Cookie cookie) {
        for (Cookie candidate : stored) {
            if (candidate.name().equals(cookie.name()) && candidate.value().equals(cookie.value())) {
                return true;
            }
        }
        return false;
    }
}
